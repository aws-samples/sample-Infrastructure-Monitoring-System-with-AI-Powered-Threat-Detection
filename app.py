#!/usr/bin/env python3
from aws_cdk import App
from video_monitoring.video_monitoring_stack import VideoMonitoringStack  # Updated import

app = App()
VideoMonitoringStack(app, "VideoMonitoringStack")  # Updated class name
app.synth()