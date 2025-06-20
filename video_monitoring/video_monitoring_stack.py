from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_sns as sns,
    aws_kms as kms,
    aws_ecr as ecr,
    aws_kinesisvideo as kvs,
    aws_s3_notifications as s3n,
    aws_logs as logs,
    RemovalPolicy,
    Duration,
    CfnOutput
)
from constructs import Construct

class VideoMonitoringStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create KMS key for flow logs
        flow_logs_key = kms.Key(
            self, "FlowLogsEncryptionKey",
            enable_key_rotation=True,
            alias=f"alias/video-vpc-flow-logs-key",
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Create VPC with proper configuration for Lambda
        vpc = ec2.Vpc(
            self, "VideoProcessingVPC",
            max_azs=2,
            nat_gateways=1,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                )
            ]
        )
        
        # Create log group for VPC flow logs
        flow_log_group = logs.LogGroup(
            self, "VPCFlowLogsGroup",
            retention=logs.RetentionDays.ONE_MONTH,
            encryption_key=flow_logs_key,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Add flow logs to the VPC
        ec2.FlowLog(
            self, "VPCFlowLog",
            resource_type=ec2.FlowLogResourceType.from_vpc(vpc),
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(flow_log_group)
        )

        # Add VPC Endpoints
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3
        )
        
        vpc.add_interface_endpoint(
            "KMSEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.KMS
        )

        vpc.add_interface_endpoint(
            "ECREndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.ECR
        )

        # Add Lambda VPC Endpoints
        vpc.add_interface_endpoint(
            "LambdaEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.LAMBDA_
        )

        vpc.add_interface_endpoint(
            "SNSEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SNS
        )

        # Create KMS keys
        bucket_key = kms.Key(
            self, "BucketEncryptionKey",
            enable_key_rotation=True,
            alias=f"alias/video-bucket-key",
            removal_policy=RemovalPolicy.DESTROY
        )

        sns_encryption_key = kms.Key(
            self, "SNSEncryptionKey",
            enable_key_rotation=True,
            alias=f"alias/video-sns-key",
            removal_policy=RemovalPolicy.DESTROY
        )

        ecr_encryption_key = kms.Key(
            self, "ECREncryptionKey",
            enable_key_rotation=True,
            alias=f"alias/video-ecr-key",
            removal_policy=RemovalPolicy.DESTROY
        )

        # Create S3 bucket
        video_bucket = s3.Bucket(
            self, "VideoProcessingBucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=bucket_key,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(7)
                )
            ],
            server_access_logs_prefix="access-logs/",
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
        )

        # Create ECR repository
        ecr_repo = ecr.Repository(
            self, "VideoProcessingRepo",
            removal_policy=RemovalPolicy.DESTROY,
            image_scan_on_push=True,
            encryption=ecr.RepositoryEncryption.KMS,
            encryption_key=ecr_encryption_key,
            image_tag_mutability=ecr.TagMutability.IMMUTABLE
        )

        # Create KVS Stream
        video_stream = kvs.CfnStream(
            self, "VideoStream",
            name=f"video-stream",
            data_retention_in_hours=24,
        )

        # Create SNS Topic
        alert_topic = sns.Topic(
            self, "ThreatAlertTopic",
            master_key=sns_encryption_key
        )

        # Create Lambda role with VPC permissions
        lambda_role = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # Add custom CloudWatch Logs permissions 
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=[
                    f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:/aws/lambda/*"
                ]
            )
        )

        # Add VPC permissions 
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                ],
                resources=[
                    f"arn:{self.partition}:ec2:{self.region}:{self.account}:*/*"
                ],
                conditions={
                    "StringEquals": {
                        "ec2:vpc": vpc.vpc_id
                    }
                }
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses"
                ],
                resources=[
                    f"arn:{self.partition}:ec2:{self.region}:{self.account}:network-interface/*"
                ],
                conditions={
                    "StringEquals": {
                        "ec2:vpc": vpc.vpc_id
                    }
                }
            )
        )

        # Add Bedrock permission
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=[
                    f"arn:{self.partition}:bedrock:{self.region}::foundation-model/amazon.nova-lite-v1:0"
                ]
            )
        )

        # Grant specific KMS permissions instead of using grant methods which create wildcards
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                ],
                resources=[
                    bucket_key.key_arn
                ]
            )
        )
        
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:ReEncrypt",
                    "kms:ReEncryptFrom",
                    "kms:ReEncryptTo"
                ],
                resources=[
                    sns_encryption_key.key_arn
                ]
            )
        )
        
        # Grant specific SNS permissions
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sns:Publish"
                ],
                resources=[
                    alert_topic.topic_arn
                ]
            )
        )
        
        # Grant specific S3 bucket and object permissions
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetBucketLocation",
                    "s3:GetBucketVersioning",
                    "s3:ListBucket"
                ],
                resources=[
                    video_bucket.bucket_arn
                ]
            )
        )
        
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:GetObjectVersion"
                ],
                resources=[
                    f"{video_bucket.bucket_arn}/*"
                ]
            )
        )

        # Create security group for Lambda
        lambda_security_group = ec2.SecurityGroup(
            self, "LambdaSecurityGroup",
            vpc=vpc,
            description="Security group for Lambda function",
            allow_all_outbound=True
        )

        # Create Lambda function with latest runtime version
        threat_detection_lambda = lambda_.Function(
            self, "ThreatDetectionFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,  # Updated to latest runtime
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("lambda"),
            role=lambda_role,
            timeout=Duration.minutes(5),
            environment={
                "SNS_TOPIC_ARN": alert_topic.topic_arn,
                "REGION": Stack.of(self).region,
                "BUCKET_KMS_KEY_ARN": bucket_key.key_arn,
                "SNS_KMS_KEY_ARN": sns_encryption_key.key_arn
            },
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_security_group]
        )

        # Add S3 event notification
        video_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(threat_detection_lambda),
        )

        # Outputs
        CfnOutput(self, "VideoStreamName", 
                 value=video_stream.name,
                 description="Name of the Kinesis Video Stream")
        
        CfnOutput(self, "BucketName", 
                 value=video_bucket.bucket_name,
                 description="Name of the S3 bucket for video processing")
        
        CfnOutput(self, "ECRRepository", 
                 value=ecr_repo.repository_uri,
                 description="URI of the ECR repository for the container image")
        
        CfnOutput(self, "SNSTopicArn", 
                 value=alert_topic.topic_arn,
                 description="ARN of the SNS topic for threat alerts")
