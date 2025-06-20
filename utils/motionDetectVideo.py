import boto3
import cv2
import os
import time
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
from botocore.exceptions import ClientError
from io import BytesIO

class KinesisVideoProcessor:
    def __init__(self):
        # Constants
        self.MIN_FRAMES = 150  # 5 seconds at 30fps
        self.MAX_FRAMES = 2700  # 90 seconds at 30fps
        self.RETRY_DELAY = 2
        self.PRE_BUFFER_SIZE = 90  # 3 seconds of pre-motion frames at 30fps
        self.MOTION_BUFFER_SIZE = 30  # Number of frames to analyze for motion
        self.MAX_RETRIES = 3
        self.LOOKBACK_SECONDS = 10
        self.MOTION_THRESHOLD = 0.5
        self.NO_MOTION_THRESHOLD = 30
        self.INACTIVE_STREAM_TIMEOUT = 5  # Seconds to wait before considering stream inactive
        
        # Initialize state
        self.stream_name = os.environ.get('KVS_STREAM_NAME')
        self.s3_bucket = os.environ.get('S3_BUCKET_NAME')
        
        if not all([self.stream_name, self.s3_bucket]):
            raise ValueError("Required environment variables KVS_STREAM_NAME and S3_BUCKET_NAME must be set")
            
        # Initialize AWS clients
        self.kvs_client = boto3.client("kinesisvideo")
        self.s3_client = boto3.client('s3')
        
        # Reset initial state
        self.reset_state()

    def reset_state(self) -> None:
        """Reset all state variables for a new recording session"""
        self.video_writer = None
        self.frames_written = 0
        self.pre_motion_buffer: List[Tuple[np.ndarray, datetime]] = []  # (frame, timestamp)
        self.motion_analysis_buffer: List[np.ndarray] = []
        self.current_output_path = None
        self.video_count = 0
        self.motion_detected = False
        self.no_motion_count = 0
        self.input_fps = 30.0
        self.prev_frame = None
        self.last_frame_time = None

    def get_stream_endpoint(self) -> Tuple[Optional[str], Optional[datetime], Optional[datetime]]:
        """Get Kinesis Video Stream endpoint for archived media"""
        try:
            end_timestamp = datetime.utcnow() - timedelta(seconds=self.LOOKBACK_SECONDS)
            start_timestamp = end_timestamp - timedelta(seconds=90)

            endpoint = self.kvs_client.get_data_endpoint(
                APIName="GET_HLS_STREAMING_SESSION_URL",
                StreamName=self.stream_name
            )['DataEndpoint']
            
            kvam = boto3.client("kinesis-video-archived-media", 
                              endpoint_url=endpoint)
            
            url = kvam.get_hls_streaming_session_url(
                StreamName=self.stream_name,
                PlaybackMode="ON_DEMAND",
                ContainerFormat='FRAGMENTED_MP4',
                DiscontinuityMode='ON_DISCONTINUITY',
                HLSFragmentSelector={
                    'FragmentSelectorType': 'SERVER_TIMESTAMP',
                    'TimestampRange': {
                        'StartTimestamp': start_timestamp,
                        'EndTimestamp': end_timestamp
                    }
                }
            )['HLSStreamingSessionURL']
            
            return url, start_timestamp, end_timestamp
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                print(f"Stream {self.stream_name} not found")
            else:
                print(f"Error accessing stream: {error_code} - {str(e)}")
            return None, None, None
        except Exception as e:
            print(f"Unexpected error getting stream endpoint: {str(e)}")
            return None, None, None

    def detect_motion(self, frame: np.ndarray) -> bool:
        """
        Detect motion using frame differencing with buffered analysis
        """
        try:
            # Add frame to motion analysis buffer
            self.motion_analysis_buffer.append(frame)
            
            # Keep only the required number of frames for analysis
            if len(self.motion_analysis_buffer) > self.MOTION_BUFFER_SIZE:
                self.motion_analysis_buffer.pop(0)
                
            # Need minimum number of frames for analysis
            if len(self.motion_analysis_buffer) < 2:
                return False
                
            # Process the latest frame
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            gray = cv2.GaussianBlur(gray, (21, 21), 0)

            if self.prev_frame is None:
                self.prev_frame = gray
                return False

            # Compute frame difference
            frame_delta = cv2.absdiff(self.prev_frame, gray)
            thresh = cv2.threshold(frame_delta, 25, 255, cv2.THRESH_BINARY)[1]
            thresh = cv2.dilate(thresh, None, iterations=2)

            self.prev_frame = gray
            motion_detected = np.mean(thresh) > self.MOTION_THRESHOLD
            
            if motion_detected:
                self.no_motion_count = 0
            else:
                self.no_motion_count += 1
                
            return motion_detected
                
        except Exception as e:
            print(f"Error in motion detection: {str(e)}")
            return False


    def start_recording(self, current_timestamp: datetime) -> None:
        """Initialize video writer with pre-motion buffer"""
        try:
            if not self.pre_motion_buffer:
                return

            self.video_count += 1
            timestamp_str = current_timestamp.strftime("%Y%m%d_%H%M%S")
            
            initial_frame = self.pre_motion_buffer[0][0]
            height, width = initial_frame.shape[:2]
            
            # Try different codec options
            codec_options = [
                ('mp4v', '.mp4'),
                ('XVID', '.avi'),
                ('MJPG', '.avi'),
                ('X264', '.mp4')
            ]
            
            for codec, ext in codec_options:
                try:
                    output_path = f"/tmp/motion_{timestamp_str}_{self.video_count}{ext}"
                    fourcc = cv2.VideoWriter_fourcc(*codec)
                    
                    writer = cv2.VideoWriter(
                        output_path,
                        fourcc,
                        self.input_fps,
                        (width, height)
                    )
                    
                    if writer is not None and writer.isOpened():
                        self.video_writer = writer
                        self.current_output_path = output_path
                        print(f"Successfully initialized codec {codec}")
                        break
                except Exception as e:
                    print(f"Failed to initialize codec {codec}: {str(e)}")
                    continue
            
            if self.video_writer is None or not self.video_writer.isOpened():
                raise RuntimeError("Failed to initialize any video codec")
                
            # Write pre-motion buffer frames
            for frame, _ in self.pre_motion_buffer:
                self.video_writer.write(frame)
                self.frames_written += 1
                
            self.pre_motion_buffer.clear()
            print(f"Started recording: {self.current_output_path} at {self.input_fps} FPS")
            
        except Exception as e:
            print(f"Error in start_recording: {str(e)}")
            if self.video_writer:
                self.video_writer.release()
                self.video_writer = None
            raise

    def process_frame(self, frame: np.ndarray, current_timestamp: datetime) -> None:
        """Process a single frame from the video stream"""
        try:
            # Update last frame time
            self.last_frame_time = time.time()
            
            # Store frame in pre-motion buffer
            self.pre_motion_buffer.append((frame.copy(), current_timestamp))
            
            # Maintain pre-motion buffer size
            while len(self.pre_motion_buffer) > self.PRE_BUFFER_SIZE:
                self.pre_motion_buffer.pop(0)

            motion_detected = self.detect_motion(frame)
            
            if not self.video_writer and motion_detected:
                self.motion_detected = True
                self.start_recording(current_timestamp)
            elif self.video_writer:
                self.video_writer.write(frame)
                self.frames_written += 1
                
                # Check stop conditions
                if self.frames_written >= self.MAX_FRAMES:
                    self.finish_recording("maximum frames reached")
                elif (self.frames_written >= self.MIN_FRAMES and 
                      self.no_motion_count > self.NO_MOTION_THRESHOLD):
                    self.finish_recording("no motion detected")
                    
        except Exception as e:
            print(f"Error processing frame: {str(e)}")
            if self.video_writer:
                self.finish_recording("error during processing")

    def finish_recording(self, reason: str) -> None:
        """Finish recording and upload to S3"""
        if not self.video_writer:
            return
            
        try:
            self.video_writer.release()
            
            timestamp = datetime.now().strftime("%Y%m%d/%H/%M%S")
            duration = self.frames_written / self.input_fps
            
            s3_key = (f"motion_videos/{timestamp}_"
                     f"frames{self.frames_written}_"
                     f"duration{duration:.1f}s.mp4")
            
            if os.path.exists(self.current_output_path) and os.path.getsize(self.current_output_path) > 0:
                self.s3_client.upload_file(
                    self.current_output_path,
                    self.s3_bucket,
                    s3_key
                )
                print(f"Uploaded video to S3: {s3_key}")
                print(f"Recording finished ({reason}): frames={self.frames_written}, duration={duration:.1f}s")
            else:
                print("Skipping upload: Empty or missing video file")
                
        except Exception as e:
            print(f"Failed to upload to S3: {str(e)}")
            
        finally:
            if os.path.exists(self.current_output_path):
                try:
                    os.remove(self.current_output_path)
                except Exception as e:
                    print(f"Failed to remove temporary file: {str(e)}")
            self.reset_state()

    def process_archived_stream(self) -> None:
        """Process archived video from the stream"""
        while True:
            stream_url, start_time, end_time = self.get_stream_endpoint()
            
            if stream_url:
                print(f"Processing archived stream from {start_time} to {end_time}")
                cap = None
                try:
                    cap = cv2.VideoCapture(stream_url)
                    if not cap.isOpened():
                        print("Failed to open stream URL")
                        time.sleep(self.RETRY_DELAY)
                        continue

                    detected_fps = cap.get(cv2.CAP_PROP_FPS)
                    if detected_fps > 0:
                        self.input_fps = detected_fps
                    print(f"Input stream FPS: {self.input_fps}")

                    frame_count = 0
                    last_frame_time = time.time()
                    frame_interval = 1.0 / self.input_fps
                    
                    while True:
                        ret, frame = cap.read()
                        
                        if not ret:
                            # Check if stream is inactive
                            if time.time() - last_frame_time > self.INACTIVE_STREAM_TIMEOUT:
                                print("Stream appears to be inactive")
                                if self.video_writer:
                                    self.finish_recording("stream inactive")
                                break
                            continue
                        
                        last_frame_time = time.time()
                        frame_count += 1
                        
                        current_timestamp = start_time + timedelta(
                            seconds=(frame_count / self.input_fps)
                        )
                        
                        self.process_frame(frame, current_timestamp)
                        
                        # Maintain proper frame timing
                        time.sleep(max(0, frame_interval - (time.time() - last_frame_time)))
                    
                except Exception as e:
                    print(f"Error processing stream: {str(e)}")
                    if self.video_writer:
                        self.finish_recording("error occurred")
                
                finally:
                    if self.video_writer:
                        self.finish_recording("processing completed")
                    if cap is not None:
                        cap.release()
            
            print(f"Waiting {self.RETRY_DELAY} seconds before processing next segment...")
            time.sleep(self.RETRY_DELAY)

def main():
    try:
        processor = KinesisVideoProcessor()
        processor.process_archived_stream()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()