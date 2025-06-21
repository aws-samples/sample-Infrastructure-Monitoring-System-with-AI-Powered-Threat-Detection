import json
import boto3
import os
import time
from urllib.parse import unquote_plus
import uuid


def analyze_video_for_threats(s3, bucket_name, file_key):
    """
    Analyze the video for potential security threats using Nova Lite.
    :param s3: Boto3 S3 client
    :param bucket_name: Name of the S3 bucket
    :param file_key: Key of the video file in S3
    :return: Analysis result as a string
    """
    bedrock_runtime = boto3.client('bedrock-runtime', region_name=os.environ.get('REGION'))
    system_list = [
        {
            "text": "You are an expert security analyst. Analyze the provided video for potential security risks."
        }
    ]
    message_list = [
        {
            "role": "user",
            "content": [
                {
                    "video": {
                        "format": "mp4",
                        "source": {
                            "s3Location": {
                                "uri": f"s3://{bucket_name}/{file_key}"
                            }
                        }
                    }
                },
                {
                    "text": """Analyze this video for potential security risks. Mark the risk field in the JSON object below as 6 or higher for the following activities [Theft, Vandalism, Assault, Burglary, Trespassing, Vehicle-related crimes, Workplace violence, Employee misconduct, Fraud attempts, Public intoxication, Unauthorized access, Arson, Harassment, Property damage]. If there is no immediate threat visible, ensure the risk score stays below 4.
Please respond with only a JSON object containing 4 keys:
1. "risk": The risk score out of 10.
2. "subject": A brief subject of the potential incident.
3. "body": A brief greeting to the security team, mentioning the potential threat.
4. "full_analysis": A detailed description of the potential incident, considering the video content."""
                }
            ]
        }
    ]
    inf_params = {"max_new_tokens": 3200, "top_p": 0.1, "top_k": 20, "temperature": 0.3}
    native_request = {
        "schemaVersion": "messages-v1",
        "messages": message_list,
        "system": system_list,
        "inferenceConfig": inf_params,
    }
    try:
        response = bedrock_runtime.invoke_model(
            modelId="amazon.nova-lite-v1:0",
            body=json.dumps(native_request)
        )
        model_response = json.loads(response["body"].read())
        analysis = model_response["output"]["message"]["content"][0]["text"]
        return analysis
    except Exception as e:
        print(f"Error in analyze_video_for_threats: {str(e)}")
        return None

def send_sns_email(subject, message):
    """
    Send an SNS email notification.
    :param subject: Email subject
    :param message: Email body
    """
    sns = boto3.client('sns')
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not topic_arn:
        print("SNS_TOPIC_ARN not set in environment variables")
        return
    try:
        response = sns.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject
        )
        print(f"SNS email sent. Message ID: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending SNS email: {str(e)}")
def lambda_handler(event, context):
    """
    Main Lambda function handler. Processes new videos uploaded to the S3 bucket.
    :param event: AWS Lambda uses this parameter to pass in event data to the handler.
    :param context: AWS Lambda uses this parameter to provide runtime information to your handler.
    :return: Lambda function result
    """
    s3 = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    if not file_key.lower().endswith('.mp4'):
        print(f"File is not an MP4 video: {file_key}")
        return {
            'statusCode': 200,
            'body': json.dumps('File is not an MP4 video')
        }
    try:
        # Analyze the video for threats
        result = analyze_video_for_threats(s3, bucket_name, file_key)
        if not result:
            print(f"No analysis result for video: {file_key}")
            return {
                'statusCode': 200,
                'body': json.dumps('No analysis result')
            }
        print(f"Analysis completed for video: {file_key}")
        print(f"Result: {result}")
        # Parse the result JSON
        result_json = json.loads(result)
        # Check if the risk level is 5 or higher
        if result_json['risk'] >= 6:
            # Send email notification
            subject = f"High Risk Alert: {result_json['subject']}"
            message = f"""

            Risk Level: {result_json['risk']}/10
            {result_json['body']}
            Full Analysis:
            {result_json['full_analysis']}
            Video: {file_key}
            """
            send_sns_email(subject, message)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'result': result,
            })
        }
    except Exception as e:
        print(f"Error processing video {file_key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing video: {str(e)}")
        }
