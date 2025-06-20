import os
import sys
import boto3
import time
import gi
import subprocess
import platform
from threading import Thread, Event
import shutil
import signal

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

class VideoStreamer:
    def __init__(self):
        self.get_user_preferences()
        self.setup_aws_credentials()
        self.check_gstreamer_setup()
        # Initialize GStreamer
        Gst.init(None)
        self.setup_video_source()
        self.stop_event = Event()
        self.pipeline = None
        self.loop = None
        self.gst_process = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def stop_streaming(self):
        """Stop the streaming process"""
        try:
            self.stop_event.set()
            if self.gst_process:
                self.gst_process.terminate()
                try:
                    self.gst_process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.gst_process.kill()
        except Exception as e:
            print(f"Error stopping stream: {e}")


    def signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown"""
        print("\nReceived shutdown signal. Stopping stream...")
        self.cleanup_resources()
        
    def cleanup_resources(self):
        """Clean up all resources"""
        try:
            self.stop_streaming()
            
            # Kill any remaining gst-launch processes
            try:
                subprocess.run(['pkill', '-f', 'gst-launch-1.0'], 
                             timeout=2,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
            except subprocess.TimeoutExpired:
                pass
            except Exception as e:
                print(f"Error killing gst processes: {e}")
                
            # Kill specific pipeline process if it exists
            if self.gst_process:
                try:
                    self.gst_process.terminate()
                    self.gst_process.wait(timeout=2)
                except:
                    try:
                        self.gst_process.kill()
                    except:
                        pass
                        
            print("All resources cleaned up")
        except Exception as e:
            print(f"Error during cleanup: {e}")

    def check_gstreamer_setup(self):
        """Check and setup GStreamer environment"""
        print("\n=== GStreamer Configuration ===")
        
        # Check if gst-launch-1.0 is in PATH
        if not shutil.which('gst-launch-1.0'):
            print("GStreamer (gst-launch-1.0) not found in PATH.")
            while True:
                gst_bin_path = input("Please enter the path to GStreamer binaries (e.g., /opt/homebrew/bin): ")
                if os.path.exists(os.path.join(gst_bin_path, 'gst-launch-1.0')):
                    # Add to PATH
                    os.environ['PATH'] = f"{gst_bin_path}:{os.environ.get('PATH', '')}"
                    print("GStreamer binaries path set successfully!")
                    break
                else:
                    print("Invalid path. gst-launch-1.0 not found in specified directory.")
        
        # Check existing GST_PLUGIN_PATH
        existing_path = os.environ.get('GST_PLUGIN_PATH')
        if existing_path and os.path.exists(existing_path):
            print(f"Found existing GST_PLUGIN_PATH: {existing_path}")
            if self.check_gstreamer_requirements():
                print("GStreamer plugin path verified successfully!")
                return
        # Common GStreamer plugin paths based on OS
        default_paths = {
            'Linux': ['/usr/lib/gstreamer-1.0', '/usr/lib/x86_64-linux-gnu/gstreamer-1.0'],
            'Darwin': ['/opt/homebrew/lib/gstreamer-1.0', '/usr/local/lib/gstreamer-1.0'],
            'Windows': ['C:\\gstreamer\\1.0\\x86_64\\lib\\gstreamer-1.0']
        }
        
        system = platform.system()
        for path in default_paths.get(system, []):
            if os.path.exists(path):
                os.environ['GST_PLUGIN_PATH'] = path
                if self.check_gstreamer_requirements():
                    print(f"Using default GStreamer plugin path: {path}")
                    return
        
        # If no working paths found, ask user
        print("No valid GStreamer plugin path found.")
        while True:
            gst_path = input("Please enter the path to your GStreamer plugins: ")
            if os.path.exists(gst_path):
                os.environ['GST_PLUGIN_PATH'] = gst_path
                if self.check_gstreamer_requirements():
                    print("GStreamer path set successfully!")
                    break
                else:
                    print("Required GStreamer plugins not found in specified directory.")
            else:
                print("Invalid path. Please enter a valid directory path.")
                
            retry = input("Would you like to try another path? (y/n): ")
            if retry.lower() != 'y':
                print("Cannot proceed without valid GStreamer setup.")
                sys.exit(1)

    def check_gstreamer_requirements(self):
        """Check if GStreamer and required plugins are installed"""
        required_plugins = ['kvssink', 'x264enc', 'videoconvert']
        missing_plugins = []
        
        try:
            for plugin in required_plugins:
                result = subprocess.run(['gst-inspect-1.0', plugin], 
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.PIPE)
                if result.returncode != 0:
                    missing_plugins.append(plugin)
                    
            if missing_plugins:
                print(f"Missing GStreamer plugins: {', '.join(missing_plugins)}")
                return False
                
            return True
            
        except Exception as e:
            print(f"Error checking GStreamer plugins: {str(e)}")
            return False

    def setup_video_source(self):
        """Determine the appropriate video source based on the operating system"""
        system = platform.system()
        if system == 'Linux':
            self.video_source = 'v4l2src device=/dev/video0'
        elif system == 'Darwin':  # macOS
            self.video_source = 'autovideosrc'
        elif system == 'Windows':
            self.video_source = 'ksvideosrc'
        else:
            self.video_source = 'autovideosrc'  # fallback
        print(f"Using video source: {self.video_source}")

    def get_user_preferences(self):
        """Get user preferences for region and stream name"""
        print("\n=== AWS Configuration ===")
        
        default_region = 'us-east-1'
        region_input = input(f"Enter AWS region (press Enter for default '{default_region}'): ").strip()
        self.region = region_input if region_input else default_region
        
        default_stream = 'video-stream'
        stream_input = input(f"Enter KVS stream name (press Enter for default '{default_stream}'): ").strip()
        self.stream_name = stream_input if stream_input else default_stream
        
        print(f"\nUsing Region: {self.region}")
        print(f"Using Stream: {self.stream_name}")

    def setup_aws_credentials(self):
        """Setup AWS credentials from AWS CLI configuration"""
        try:
            session = boto3.Session(region_name=self.region)
            credentials = session.get_credentials()
            
            if not credentials:
                print("Error: No AWS credentials found. Please configure AWS CLI.")
                sys.exit(1)
                
            os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
            
            if credentials.token:
                os.environ['AWS_SESSION_TOKEN'] = credentials.token
                
            os.environ['AWS_REGION'] = self.region
            os.environ['KVS_STREAM_NAME'] = self.stream_name
            
            self.verify_kvs_stream()
            print("\nAWS Configuration verified successfully!")
            
        except Exception as e:
            print("Error accessing AWS credentials. Please ensure AWS CLI is configured.")
            print(f"Error details: {str(e)}")
            sys.exit(1)
    def verify_kvs_stream(self):
        """Verify KVS stream exists or create it"""
        try:
            kvs_client = boto3.client('kinesisvideo', region_name=self.region)
            try:
                kvs_client.describe_stream(StreamName=self.stream_name)
            except kvs_client.exceptions.ResourceNotFoundException:
                print(f"Creating new KVS stream: {self.stream_name}")
                kvs_client.create_stream(
                    StreamName=self.stream_name,
                    DataRetentionInHours=2,
                    MediaType='video/h264'
                )
                # Wait for stream to become active
                waiter = kvs_client.get_waiter('stream_active')
                waiter.wait(StreamName=self.stream_name)
        except Exception as e:
            print(f"Error verifying KVS stream: {str(e)}")
            raise

    def create_pipeline(self, source_element):
        """Create GStreamer pipeline string"""
        pipeline_str = (
            f"{source_element} ! "
            f"videoconvert ! "
            f"x264enc bframes=0 key-int-max=45 bitrate=500 ! "
            f"video/x-h264,profile=baseline,stream-format=avc,alignment=au ! "
            f"kvssink stream-name={self.stream_name} storage-size=512 "
            f"aws-region={self.region}"
        )
        return pipeline_str

    def run_gstreamer_pipeline(self, pipeline_str):
        """Run GStreamer pipeline using gst-launch"""
        try:
            # Kill any existing gst-launch processes
            try:
                subprocess.run(['pkill', '-f', 'gst-launch-1.0'], 
                            timeout=2,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
            except:
                pass

            # Start new pipeline process
            self.gst_process = subprocess.Popen(
                ['gst-launch-1.0', '-v'] + pipeline_str.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            print("\nStreaming started... Press Ctrl+C to stop")
            
            while not self.stop_event.is_set():
                return_code = self.gst_process.poll()
                if return_code is not None:
                    # Pipeline has finished or encountered an error
                    error = self.gst_process.stderr.read().decode()
                    if error and return_code != 0:
                        print(f"Pipeline error: {error}")
                    elif return_code == 0:
                        print("\nStream completed successfully")
                    break
                time.sleep(0.1)
                    
        except KeyboardInterrupt:
            print("\nStreaming interrupted by user")
        finally:
            self.cleanup_resources()


    def stream_from_webcam(self):
        """Stream from webcam source"""
        try:
            pipeline_str = self.create_pipeline(self.video_source)
            self.stop_event.clear()
            self.run_gstreamer_pipeline(pipeline_str)
        except Exception as e:
            print(f"Error in webcam stream: {str(e)}")
        finally:
            input("\nPress Enter to return to main menu...")

    def stream_from_file(self):
        """Stream from local video file"""
        while True:
            file_path = input("\nEnter the path to your video file: ")
            if os.path.exists(file_path):
                try:
                    source_element = f'filesrc location="{file_path}" ! decodebin'
                    pipeline_str = self.create_pipeline(source_element)
                    self.stop_event.clear()
                    self.run_gstreamer_pipeline(pipeline_str)
                    print("\nStreaming session ended")
                    break
                except Exception as e:
                    print(f"Error in file stream: {str(e)}")
                    break
            else:
                print("File not found. Please enter a valid file path.")
                retry = input("Would you like to try another file? (y/n): ")
                if retry.lower() != 'y':
                    break
        
        print("\nReturning to main menu...")
        time.sleep(1)  # Give user time to read the message


def main():
    """Main application entry point"""
    print("\nWelcome to KVS Video Streaming Application!")
    
    try:
        streamer = VideoStreamer()
        
        while True:
            print("\n=== Video Streaming Application ===")
            print("1. Stream from Webcam")
            print("2. Stream from Local File")
            print("3. Quit Application")
            print("================================")
            
            choice = input("\nEnter your choice (1-3): ")
            
            if choice == '1':
                streamer.stream_from_webcam()
            elif choice == '2':
                streamer.stream_from_file()
            elif choice == '3':
                print("\nThank you for using the Video Streaming Application!")
                break
            else:
                print("\nInvalid choice. Please enter 1, 2, or 3.")
                input("Press Enter to continue...")

    except KeyboardInterrupt:
        print("\nApplication terminated by user")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
    finally:
        print("\nApplication closed")

if __name__ == "__main__":
    main()

