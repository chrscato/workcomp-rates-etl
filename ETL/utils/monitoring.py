"""
ETL Monitoring and Logging Utilities

This module provides utilities for monitoring ETL pipeline performance and data quality.
"""

import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ETLMonitor:
    """ETL pipeline monitoring and metrics collection."""
    
    def __init__(self, s3_bucket: str, region: str = 'us-east-1'):
        self.s3_bucket = s3_bucket
        self.region = region
        
        # Initialize AWS clients
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        
        # Monitoring configuration
        self.namespace = 'HealthcareDataLake/ETL3'
        self.metrics = []
        
        logger.info(f"ETL Monitor initialized for bucket: {s3_bucket}")
    
    def start_pipeline_monitoring(self, pipeline_id: str) -> str:
        """Start monitoring a pipeline execution."""
        
        start_time = datetime.now()
        self.pipeline_id = pipeline_id
        self.start_time = start_time
        
        logger.info(f"Starting pipeline monitoring: {pipeline_id}")
        
        # Record pipeline start
        self._record_metric('PipelineStart', 1, 'Count', {
            'PipelineId': pipeline_id,
            'Status': 'Started'
        })
        
        return pipeline_id
    
    def record_chunk_processing(self, chunk_num: int, input_rows: int, output_rows: int, 
                              processing_time: float, partitions_created: int) -> None:
        """Record chunk processing metrics."""
        
        self._record_metric('ChunkProcessingTime', processing_time, 'Seconds', {
            'PipelineId': self.pipeline_id,
            'ChunkNumber': str(chunk_num)
        })
        
        self._record_metric('ChunkInputRows', input_rows, 'Count', {
            'PipelineId': self.pipeline_id,
            'ChunkNumber': str(chunk_num)
        })
        
        self._record_metric('ChunkOutputRows', output_rows, 'Count', {
            'PipelineId': self.pipeline_id,
            'ChunkNumber': str(chunk_num)
        })
        
        self._record_metric('PartitionsCreated', partitions_created, 'Count', {
            'PipelineId': self.pipeline_id,
            'ChunkNumber': str(chunk_num)
        })
        
        # Calculate processing rate
        if processing_time > 0:
            rows_per_second = input_rows / processing_time
            self._record_metric('ChunkProcessingRate', rows_per_second, 'Count/Second', {
                'PipelineId': self.pipeline_id,
                'ChunkNumber': str(chunk_num)
            })
    
    def record_data_quality(self, partition_path: str, quality_score: float, 
                          error_count: int, warning_count: int) -> None:
        """Record data quality metrics for a partition."""
        
        self._record_metric('DataQualityScore', quality_score, 'Percent', {
            'PipelineId': self.pipeline_id,
            'PartitionPath': partition_path
        })
        
        self._record_metric('DataQualityErrors', error_count, 'Count', {
            'PipelineId': self.pipeline_id,
            'PartitionPath': partition_path
        })
        
        self._record_metric('DataQualityWarnings', warning_count, 'Count', {
            'PipelineId': self.pipeline_id,
            'PartitionPath': partition_path
        })
    
    def record_s3_operation(self, operation: str, success: bool, 
                          file_size: int, processing_time: float) -> None:
        """Record S3 operation metrics."""
        
        status = 'Success' if success else 'Failed'
        
        self._record_metric('S3Operation', 1, 'Count', {
            'PipelineId': self.pipeline_id,
            'Operation': operation,
            'Status': status
        })
        
        if success:
            self._record_metric('S3FileSize', file_size, 'Bytes', {
                'PipelineId': self.pipeline_id,
                'Operation': operation
            })
            
            self._record_metric('S3OperationTime', processing_time, 'Seconds', {
                'PipelineId': self.pipeline_id,
                'Operation': operation
            })
    
    def record_memory_usage(self, memory_mb: float) -> None:
        """Record memory usage metrics."""
        
        self._record_metric('MemoryUsage', memory_mb, 'Megabytes', {
            'PipelineId': self.pipeline_id
        })
    
    def record_error(self, error_type: str, error_message: str, 
                    component: str) -> None:
        """Record error metrics."""
        
        self._record_metric('ErrorCount', 1, 'Count', {
            'PipelineId': self.pipeline_id,
            'ErrorType': error_type,
            'Component': component
        })
        
        logger.error(f"Error recorded: {error_type} in {component}: {error_message}")
    
    def end_pipeline_monitoring(self, status: str, total_rows: int, 
                              total_partitions: int, total_errors: int = 0) -> Dict[str, Any]:
        """End pipeline monitoring and generate summary."""
        
        end_time = datetime.now()
        total_time = (end_time - self.start_time).total_seconds()
        
        # Record final metrics
        self._record_metric('PipelineEnd', 1, 'Count', {
            'PipelineId': self.pipeline_id,
            'Status': status
        })
        
        self._record_metric('TotalProcessingTime', total_time, 'Seconds', {
            'PipelineId': self.pipeline_id
        })
        
        self._record_metric('TotalRowsProcessed', total_rows, 'Count', {
            'PipelineId': self.pipeline_id
        })
        
        self._record_metric('TotalPartitionsCreated', total_partitions, 'Count', {
            'PipelineId': self.pipeline_id
        })
        
        if total_errors > 0:
            self._record_metric('TotalErrors', total_errors, 'Count', {
                'PipelineId': self.pipeline_id
            })
        
        # Calculate processing rate
        if total_time > 0:
            rows_per_second = total_rows / total_time
            self._record_metric('OverallProcessingRate', rows_per_second, 'Count/Second', {
                'PipelineId': self.pipeline_id
            })
        
        # Send all metrics to CloudWatch
        self._send_metrics_to_cloudwatch()
        
        # Generate summary
        summary = {
            'pipeline_id': self.pipeline_id,
            'status': status,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'total_time_seconds': total_time,
            'total_rows_processed': total_rows,
            'total_partitions_created': total_partitions,
            'total_errors': total_errors,
            'rows_per_second': total_rows / total_time if total_time > 0 else 0,
            'metrics_recorded': len(self.metrics)
        }
        
        logger.info(f"Pipeline monitoring completed: {status}")
        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Total partitions: {total_partitions:,}")
        logger.info(f"Processing rate: {summary['rows_per_second']:.0f} rows/second")
        
        return summary
    
    def _record_metric(self, metric_name: str, value: float, unit: str, 
                      dimensions: Dict[str, str]) -> None:
        """Record a metric for later sending to CloudWatch."""
        
        metric = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Dimensions': [
                {'Name': k, 'Value': v} for k, v in dimensions.items()
            ],
            'Timestamp': datetime.now()
        }
        
        self.metrics.append(metric)
        logger.debug(f"Recorded metric: {metric_name} = {value} {unit}")
    
    def _send_metrics_to_cloudwatch(self) -> None:
        """Send all recorded metrics to CloudWatch."""
        
        if not self.metrics:
            return
        
        logger.info(f"Sending {len(self.metrics)} metrics to CloudWatch...")
        
        try:
            # CloudWatch allows up to 20 metrics per request
            batch_size = 20
            for i in range(0, len(self.metrics), batch_size):
                batch = self.metrics[i:i + batch_size]
                
                self.cloudwatch_client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
            
            logger.info("Metrics sent to CloudWatch successfully")
            
        except ClientError as e:
            logger.error(f"Failed to send metrics to CloudWatch: {e}")
            raise
    
    def get_pipeline_metrics(self, pipeline_id: str, 
                           start_time: datetime, 
                           end_time: datetime) -> Dict[str, Any]:
        """Retrieve pipeline metrics from CloudWatch."""
        
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace=self.namespace,
                MetricName='PipelineEnd',
                Dimensions=[
                    {
                        'Name': 'PipelineId',
                        'Value': pipeline_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5 minutes
                Statistics=['Sum', 'Average', 'Maximum', 'Minimum']
            )
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to retrieve metrics from CloudWatch: {e}")
            return {}
    
    def create_dashboard(self, pipeline_id: str) -> str:
        """Create a CloudWatch dashboard for the pipeline."""
        
        dashboard_name = f"ETL3-Pipeline-{pipeline_id}"
        
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
                            [self.namespace, "ChunkProcessingRate", "PipelineId", pipeline_id],
                            [".", "OverallProcessingRate", ".", "."]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Processing Rate",
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
                            [self.namespace, "ChunkInputRows", "PipelineId", pipeline_id],
                            [".", "ChunkOutputRows", ".", "."]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Rows Processed",
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
                            [self.namespace, "DataQualityScore", "PipelineId", pipeline_id]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Data Quality Score",
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
                            [self.namespace, "ErrorCount", "PipelineId", pipeline_id]
                        ],
                        "view": "timeSeries",
                        "stacked": False,
                        "region": self.region,
                        "title": "Error Count",
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
            
            logger.info(f"Dashboard created: {dashboard_name}")
            return dashboard_name
            
        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def setup_alarms(self, pipeline_id: str) -> List[str]:
        """Set up CloudWatch alarms for the pipeline."""
        
        alarms = []
        
        # High error rate alarm
        error_alarm_name = f"ETL3-HighErrorRate-{pipeline_id}"
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=error_alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='ErrorCount',
                Namespace=self.namespace,
                Period=300,
                Statistic='Sum',
                Threshold=10.0,
                ActionsEnabled=True,
                AlarmDescription='High error rate in ETL3 pipeline',
                Dimensions=[
                    {
                        'Name': 'PipelineId',
                        'Value': pipeline_id
                    }
                ]
            )
            alarms.append(error_alarm_name)
            
        except ClientError as e:
            logger.error(f"Failed to create error alarm: {e}")
        
        # Low processing rate alarm
        rate_alarm_name = f"ETL3-LowProcessingRate-{pipeline_id}"
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=rate_alarm_name,
                ComparisonOperator='LessThanThreshold',
                EvaluationPeriods=2,
                MetricName='ChunkProcessingRate',
                Namespace=self.namespace,
                Period=300,
                Statistic='Average',
                Threshold=100.0,
                ActionsEnabled=True,
                AlarmDescription='Low processing rate in ETL3 pipeline',
                Dimensions=[
                    {
                        'Name': 'PipelineId',
                        'Value': pipeline_id
                    }
                ]
            )
            alarms.append(rate_alarm_name)
            
        except ClientError as e:
            logger.error(f"Failed to create rate alarm: {e}")
        
        logger.info(f"Created {len(alarms)} CloudWatch alarms")
        return alarms
    
    def generate_report(self, pipeline_id: str, output_path: Path) -> Dict[str, Any]:
        """Generate a comprehensive monitoring report."""
        
        report = {
            'pipeline_id': pipeline_id,
            'generated_at': datetime.now().isoformat(),
            'summary': {},
            'metrics': {},
            'recommendations': []
        }
        
        # Get recent metrics
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        metrics = self.get_pipeline_metrics(pipeline_id, start_time, end_time)
        
        if metrics:
            report['metrics'] = metrics
            report['summary'] = {
                'total_datapoints': len(metrics.get('Datapoints', [])),
                'time_range': f"{start_time.isoformat()} to {end_time.isoformat()}"
            }
        
        # Generate recommendations
        if 'Datapoints' in metrics:
            datapoints = metrics['Datapoints']
            if datapoints:
                avg_processing_rate = sum(dp['Average'] for dp in datapoints) / len(datapoints)
                
                if avg_processing_rate < 1000:
                    report['recommendations'].append(
                        "Consider optimizing processing rate - current average is below 1000 rows/second"
                    )
        
        # Save report
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Monitoring report saved to: {output_path}")
        return report
