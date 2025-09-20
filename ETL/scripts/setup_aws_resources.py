#!/usr/bin/env python3
"""
AWS Resources Setup Script

This script sets up the necessary AWS resources for the ETL3 pipeline.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "ETL" / "utils"))

import boto3
from botocore.exceptions import ClientError
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AWSResourceSetup:
    """Set up AWS resources for ETL3 pipeline."""
    
    def __init__(self, region='us-east-1'):
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
        self.athena_client = boto3.client('athena', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        
        # Get account ID
        sts_client = boto3.client('sts', region_name=region)
        self.account_id = sts_client.get_caller_identity()['Account']
        
        logger.info(f"AWS Resource Setup initialized for region: {region}")
    
    def create_s3_bucket(self, bucket_name: str) -> bool:
        """Create S3 bucket with proper configuration."""
        
        logger.info(f"Creating S3 bucket: {bucket_name}")
        
        try:
            # Create bucket
            if self.region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            
            # Enable versioning
            self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            # Enable server-side encryption
            self.s3_client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        }
                    ]
                }
            )
            
            # Block public access
            self.s3_client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                }
            )
            
            # Set up lifecycle policy
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={
                    'Rules': [
                        {
                            'ID': 'healthcare-data-lifecycle',
                            'Status': 'Enabled',
                            'Filter': {
                                'Prefix': 'partitioned-data/'
                            },
                            'Transitions': [
                                {
                                    'Days': 30,
                                    'StorageClass': 'STANDARD_IA'
                                },
                                {
                                    'Days': 90,
                                    'StorageClass': 'GLACIER'
                                },
                                {
                                    'Days': 365,
                                    'StorageClass': 'DEEP_ARCHIVE'
                                }
                            ]
                        }
                    ]
                }
            )
            
            logger.info(f"✅ S3 bucket created and configured: {bucket_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                logger.warning(f"S3 bucket already exists: {bucket_name}")
                return True
            else:
                logger.error(f"Failed to create S3 bucket: {e}")
                return False
    
    def create_iam_role(self, role_name: str) -> str:
        """Create IAM role for Glue service."""
        
        logger.info(f"Creating IAM role: {role_name}")
        
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
        role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::*",
                        f"arn:aws:s3:::*/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:StopQueryExecution"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetTable",
                        "glue:GetDatabase",
                        "glue:GetPartitions",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                        "glue:DeleteTable",
                        "glue:GetCrawler",
                        "glue:StartCrawler",
                        "glue:StopCrawler"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "cloudwatch:PutMetricData",
                        "cloudwatch:GetMetricStatistics",
                        "cloudwatch:PutDashboard"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        try:
            # Create role
            self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='Role for ETL3 Glue operations'
            )
            
            # Attach policies
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='ETL3Policy',
                PolicyDocument=json.dumps(role_policy)
            )
            
            # Attach AWS managed policies
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
            )
            
            role_arn = f"arn:aws:iam::{self.account_id}:role/{role_name}"
            logger.info(f"✅ IAM role created: {role_arn}")
            return role_arn
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.warning(f"IAM role already exists: {role_name}")
                return f"arn:aws:iam::{self.account_id}:role/{role_name}"
            else:
                logger.error(f"Failed to create IAM role: {e}")
                raise
    
    def create_athena_database(self, database_name: str) -> bool:
        """Create Athena database."""
        
        logger.info(f"Creating Athena database: {database_name}")
        
        try:
            self.athena_client.start_query_execution(
                QueryString=f"CREATE DATABASE IF NOT EXISTS {database_name}",
                ResultConfiguration={
                    'OutputLocation': f's3://healthcare-data-lake-{self.region}/athena-results/'
                }
            )
            
            logger.info(f"✅ Athena database created: {database_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create Athena database: {e}")
            return False
    
    def create_glue_crawler(self, crawler_name: str, database_name: str, 
                           s3_path: str, role_arn: str) -> bool:
        """Create Glue crawler."""
        
        logger.info(f"Creating Glue crawler: {crawler_name}")
        
        try:
            self.glue_client.create_crawler(
                Name=crawler_name,
                Role=role_arn,
                DatabaseName=database_name,
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_path,
                            'Exclusions': ['**/athena-results/**']
                        }
                    ]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                }
            )
            
            logger.info(f"✅ Glue crawler created: {crawler_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.warning(f"Glue crawler already exists: {crawler_name}")
                return True
            else:
                logger.error(f"Failed to create Glue crawler: {e}")
                return False
    
    def create_cloudwatch_dashboard(self, dashboard_name: str) -> bool:
        """Create CloudWatch dashboard."""
        
        logger.info(f"Creating CloudWatch dashboard: {dashboard_name}")
        
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            ["HealthcareDataLake/ETL3", "ChunkProcessingRate"],
                            [".", "OverallProcessingRate"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "ETL3 Processing Rate",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            ["HealthcareDataLake/ETL3", "ChunkInputRows"],
                            [".", "ChunkOutputRows"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "ETL3 Rows Processed",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 0,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            ["HealthcareDataLake/ETL3", "DataQualityScore"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "ETL3 Data Quality Score",
                        "period": 300
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "metrics": [
                            ["HealthcareDataLake/ETL3", "ErrorCount"]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "ETL3 Error Count",
                        "period": 300
                    }
                }
            ]
        }
        
        try:
            self.cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=json.dumps(dashboard_body)
            )
            
            logger.info(f"✅ CloudWatch dashboard created: {dashboard_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to create CloudWatch dashboard: {e}")
            return False
    
    def setup_all_resources(self, bucket_name: str, environment: str = 'development') -> Dict[str, Any]:
        """Set up all AWS resources for ETL3 pipeline."""
        
        logger.info(f"Setting up AWS resources for environment: {environment}")
        
        results = {
            'bucket_created': False,
            'role_created': False,
            'database_created': False,
            'crawler_created': False,
            'dashboard_created': False,
            'errors': []
        }
        
        try:
            # Create S3 bucket
            results['bucket_created'] = self.create_s3_bucket(bucket_name)
            
            # Create IAM role
            role_name = f"ETL3-GlueRole-{environment}"
            role_arn = self.create_iam_role(role_name)
            results['role_created'] = True
            
            # Create Athena database
            database_name = f"healthcare_data_lake_{environment}"
            results['database_created'] = self.create_athena_database(database_name)
            
            # Create Glue crawler
            crawler_name = f"fact_rate_enriched_crawler_{environment}"
            s3_path = f"s3://{bucket_name}/partitioned-data/"
            results['crawler_created'] = self.create_glue_crawler(
                crawler_name, database_name, s3_path, role_arn
            )
            
            # Create CloudWatch dashboard
            dashboard_name = f"ETL3-Pipeline-{environment}"
            results['dashboard_created'] = self.create_cloudwatch_dashboard(dashboard_name)
            
            logger.info("🎉 All AWS resources set up successfully!")
            
        except Exception as e:
            logger.error(f"Failed to set up AWS resources: {e}")
            results['errors'].append(str(e))
        
        return results


def main():
    """Main function."""
    
    parser = argparse.ArgumentParser(description='Set up AWS resources for ETL3 pipeline')
    parser.add_argument('--bucket-name', required=True, help='S3 bucket name')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--environment', default='development', 
                       choices=['development', 'staging', 'production'],
                       help='Environment name')
    
    args = parser.parse_args()
    
    # Set up AWS resources
    setup = AWSResourceSetup(region=args.region)
    results = setup.setup_all_resources(args.bucket_name, args.environment)
    
    # Display results
    print("\n" + "="*60)
    print("AWS RESOURCES SETUP RESULTS")
    print("="*60)
    print(f"Environment: {args.environment}")
    print(f"Region: {args.region}")
    print(f"S3 Bucket: {args.bucket_name}")
    print(f"Bucket Created: {'✅' if results['bucket_created'] else '❌'}")
    print(f"IAM Role Created: {'✅' if results['role_created'] else '❌'}")
    print(f"Athena Database Created: {'✅' if results['database_created'] else '❌'}")
    print(f"Glue Crawler Created: {'✅' if results['crawler_created'] else '❌'}")
    print(f"CloudWatch Dashboard Created: {'✅' if results['dashboard_created'] else '❌'}")
    
    if results['errors']:
        print("\nErrors:")
        for error in results['errors']:
            print(f"  - {error}")
    
    print("="*60)
    
    # Exit with error code if any failures
    if not all([results['bucket_created'], results['role_created'], 
                results['database_created'], results['crawler_created']]):
        sys.exit(1)


if __name__ == "__main__":
    main()
