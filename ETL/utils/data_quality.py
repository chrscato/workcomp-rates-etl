"""
Data Quality Validation Utilities

This module provides utilities for validating data quality in the ETL pipeline.
"""

import logging
from typing import Dict, List, Any, Optional
import polars as pl
import numpy as np

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Data quality validation for ETL pipeline."""
    
    def __init__(self):
        self.quality_rules = self._define_quality_rules()
    
    def _define_quality_rules(self) -> Dict[str, Dict[str, Any]]:
        """Define data quality rules for validation."""
        
        return {
            'required_fields': {
                'fields': ['fact_uid', 'negotiated_rate', 'state', 'payer_slug'],
                'description': 'Required fields must not be null'
            },
            'rate_validation': {
                'field': 'negotiated_rate',
                'min_value': 0.01,
                'max_value': 1000000.0,
                'description': 'Negotiated rates must be between $0.01 and $1,000,000'
            },
            'state_validation': {
                'field': 'state',
                'valid_values': ['GA', 'FL', 'CA', 'TX', 'NY', 'IL', 'PA', 'OH', 'NC', 'MI'],
                'description': 'State must be a valid US state code'
            },
            'payer_validation': {
                'field': 'payer_slug',
                'pattern': r'^[a-z0-9-]+$',
                'description': 'Payer slug must contain only lowercase letters, numbers, and hyphens'
            },
            'npi_validation': {
                'field': 'npi',
                'pattern': r'^\d{10}$',
                'description': 'NPI must be exactly 10 digits'
            },
            'geocoding_validation': {
                'latitude_range': (-90, 90),
                'longitude_range': (-180, 180),
                'description': 'Latitude and longitude must be within valid ranges'
            }
        }
    
    def validate_partition(self, partition_data: pl.DataFrame) -> Dict[str, Any]:
        """Validate data quality for a single partition."""
        
        logger.info(f"Validating partition with {partition_data.height:,} rows...")
        
        validation_results = {
            'partition_rows': partition_data.height,
            'has_errors': False,
            'errors': [],
            'metrics': {},
            'warnings': []
        }
        
        # Check required fields
        required_errors = self._check_required_fields(partition_data)
        if required_errors:
            validation_results['errors'].extend(required_errors)
            validation_results['has_errors'] = True
        
        # Check rate validation
        rate_errors = self._check_rate_validation(partition_data)
        if rate_errors:
            validation_results['errors'].extend(rate_errors)
            validation_results['has_errors'] = True
        
        # Check state validation
        state_errors = self._check_state_validation(partition_data)
        if state_errors:
            validation_results['errors'].extend(state_errors)
            validation_results['has_errors'] = True
        
        # Check payer validation
        payer_errors = self._check_payer_validation(partition_data)
        if payer_errors:
            validation_results['errors'].extend(payer_errors)
            validation_results['has_errors'] = True
        
        # Check NPI validation
        npi_errors = self._check_npi_validation(partition_data)
        if npi_errors:
            validation_results['errors'].extend(npi_errors)
            validation_results['has_errors'] = True
        
        # Check geocoding validation
        geo_errors = self._check_geocoding_validation(partition_data)
        if geo_errors:
            validation_results['warnings'].extend(geo_errors)  # Warnings, not errors
        
        # Calculate quality metrics
        validation_results['metrics'] = self._calculate_quality_metrics(partition_data)
        
        logger.info(f"Validation completed: {len(validation_results['errors'])} errors, {len(validation_results['warnings'])} warnings")
        
        return validation_results
    
    def _check_required_fields(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Check that required fields are not null."""
        
        errors = []
        required_fields = self.quality_rules['required_fields']['fields']
        
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(pl.col(field).is_null()).height
                if null_count > 0:
                    errors.append({
                        'rule': 'required_fields',
                        'field': field,
                        'error': f'{null_count:,} rows have null values in required field: {field}',
                        'severity': 'error'
                    })
        
        return errors
    
    def _check_rate_validation(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Check negotiated rate validation."""
        
        errors = []
        field = self.quality_rules['rate_validation']['field']
        min_value = self.quality_rules['rate_validation']['min_value']
        max_value = self.quality_rules['rate_validation']['max_value']
        
        if field in df.columns:
            # Check for negative rates
            negative_count = df.filter(pl.col(field) < 0).height
            if negative_count > 0:
                errors.append({
                    'rule': 'rate_validation',
                    'field': field,
                    'error': f'{negative_count:,} rows have negative rates',
                    'severity': 'error'
                })
            
            # Check for rates outside valid range
            invalid_range_count = df.filter(
                (pl.col(field) < min_value) | (pl.col(field) > max_value)
            ).height
            if invalid_range_count > 0:
                errors.append({
                    'rule': 'rate_validation',
                    'field': field,
                    'error': f'{invalid_range_count:,} rows have rates outside valid range (${min_value}-${max_value})',
                    'severity': 'error'
                })
        
        return errors
    
    def _check_state_validation(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Check state code validation."""
        
        errors = []
        field = self.quality_rules['state_validation']['field']
        valid_values = self.quality_rules['state_validation']['valid_values']
        
        if field in df.columns:
            invalid_states = df.filter(~pl.col(field).is_in(valid_values)).height
            if invalid_states > 0:
                errors.append({
                    'rule': 'state_validation',
                    'field': field,
                    'error': f'{invalid_states:,} rows have invalid state codes',
                    'severity': 'error'
                })
        
        return errors
    
    def _check_payer_validation(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Check payer slug validation."""
        
        errors = []
        field = self.quality_rules['payer_validation']['field']
        pattern = self.quality_rules['payer_validation']['pattern']
        
        if field in df.columns:
            # Check for invalid characters in payer slug
            invalid_payers = df.filter(
                pl.col(field).str.contains(r'[^a-z0-9-]')
            ).height
            if invalid_payers > 0:
                errors.append({
                    'rule': 'payer_validation',
                    'field': field,
                    'error': f'{invalid_payers:,} rows have invalid payer slug format',
                    'severity': 'error'
                })
        
        return errors
    
    def _check_npi_validation(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Check NPI validation."""
        
        errors = []
        field = self.quality_rules['npi_validation']['field']
        pattern = self.quality_rules['npi_validation']['pattern']
        
        if field in df.columns:
            # Check NPI format (10 digits)
            invalid_npis = df.filter(
                pl.col(field).is_not_null() & 
                ~pl.col(field).str.contains(pattern)
            ).height
            if invalid_npis > 0:
                errors.append({
                    'rule': 'npi_validation',
                    'field': field,
                    'error': f'{invalid_npis:,} rows have invalid NPI format',
                    'severity': 'error'
                })
        
        return errors
    
    def _check_geocoding_validation(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Check geocoding validation."""
        
        warnings = []
        lat_range = self.quality_rules['geocoding_validation']['latitude_range']
        lon_range = self.quality_rules['geocoding_validation']['longitude_range']
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Check latitude range
            invalid_lat = df.filter(
                pl.col('latitude').is_not_null() & 
                ((pl.col('latitude') < lat_range[0]) | (pl.col('latitude') > lat_range[1]))
            ).height
            if invalid_lat > 0:
                warnings.append({
                    'rule': 'geocoding_validation',
                    'field': 'latitude',
                    'warning': f'{invalid_lat:,} rows have invalid latitude values',
                    'severity': 'warning'
                })
            
            # Check longitude range
            invalid_lon = df.filter(
                pl.col('longitude').is_not_null() & 
                ((pl.col('longitude') < lon_range[0]) | (pl.col('longitude') > lon_range[1]))
            ).height
            if invalid_lon > 0:
                warnings.append({
                    'rule': 'geocoding_validation',
                    'field': 'longitude',
                    'warning': f'{invalid_lon:,} rows have invalid longitude values',
                    'severity': 'warning'
                })
        
        return warnings
    
    def _calculate_quality_metrics(self, df: pl.DataFrame) -> Dict[str, float]:
        """Calculate data quality metrics."""
        
        metrics = {}
        
        # Completeness metrics
        total_rows = df.height
        if total_rows > 0:
            for field in ['fact_uid', 'negotiated_rate', 'state', 'payer_slug']:
                if field in df.columns:
                    null_count = df.filter(pl.col(field).is_null()).height
                    completeness = (total_rows - null_count) / total_rows
                    metrics[f'{field}_completeness'] = completeness
        
        # Rate statistics
        if 'negotiated_rate' in df.columns:
            rate_stats = df.select(pl.col('negotiated_rate')).describe()
            metrics['avg_rate'] = rate_stats['mean'].item() if 'mean' in rate_stats.columns else 0
            metrics['median_rate'] = rate_stats['median'].item() if 'median' in rate_stats.columns else 0
            metrics['min_rate'] = rate_stats['min'].item() if 'min' in rate_stats.columns else 0
            metrics['max_rate'] = rate_stats['max'].item() if 'max' in rate_stats.columns else 0
        
        # Geographic coverage
        if 'stat_area_name' in df.columns:
            unique_areas = df.select('stat_area_name').n_unique()
            metrics['unique_stat_areas'] = unique_areas
        
        if 'county_name' in df.columns:
            unique_counties = df.select('county_name').n_unique()
            metrics['unique_counties'] = unique_counties
        
        # Provider coverage
        if 'npi' in df.columns:
            unique_npis = df.select('npi').n_unique()
            metrics['unique_npis'] = unique_npis
        
        # Medicare benchmark coverage
        if 'medicare_state_rate' in df.columns:
            benchmark_coverage = df.filter(pl.col('medicare_state_rate').is_not_null()).height / total_rows
            metrics['medicare_benchmark_coverage'] = benchmark_coverage
        
        return metrics
    
    def generate_quality_report(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a comprehensive quality report from validation results."""
        
        total_partitions = len(validation_results)
        total_rows = sum(result['partition_rows'] for result in validation_results)
        
        # Aggregate errors and warnings
        all_errors = []
        all_warnings = []
        
        for result in validation_results:
            all_errors.extend(result.get('errors', []))
            all_warnings.extend(result.get('warnings', []))
        
        # Calculate overall metrics
        overall_metrics = {}
        if validation_results:
            # Average completeness across all partitions
            for field in ['fact_uid', 'negotiated_rate', 'state', 'payer_slug']:
                completeness_values = [
                    result['metrics'].get(f'{field}_completeness', 0) 
                    for result in validation_results 
                    if f'{field}_completeness' in result['metrics']
                ]
                if completeness_values:
                    overall_metrics[f'{field}_completeness'] = sum(completeness_values) / len(completeness_values)
        
        # Quality score calculation
        error_count = len(all_errors)
        warning_count = len(all_warnings)
        quality_score = max(0, 100 - (error_count * 10) - (warning_count * 2))
        
        report = {
            'summary': {
                'total_partitions': total_partitions,
                'total_rows': total_rows,
                'error_count': error_count,
                'warning_count': warning_count,
                'quality_score': quality_score
            },
            'errors': all_errors,
            'warnings': all_warnings,
            'metrics': overall_metrics,
            'recommendations': self._generate_recommendations(all_errors, all_warnings)
        }
        
        return report
    
    def _generate_recommendations(self, errors: List[Dict], warnings: List[Dict]) -> List[str]:
        """Generate recommendations based on validation results."""
        
        recommendations = []
        
        # Error-based recommendations
        error_types = set(error['rule'] for error in errors)
        
        if 'required_fields' in error_types:
            recommendations.append("Review data sources for missing required fields")
        
        if 'rate_validation' in error_types:
            recommendations.append("Implement rate validation in upstream data processing")
        
        if 'state_validation' in error_types:
            recommendations.append("Add state code validation to data ingestion")
        
        if 'payer_validation' in error_types:
            recommendations.append("Standardize payer slug format in data processing")
        
        if 'npi_validation' in error_types:
            recommendations.append("Implement NPI format validation")
        
        # Warning-based recommendations
        warning_types = set(warning['rule'] for warning in warnings)
        
        if 'geocoding_validation' in warning_types:
            recommendations.append("Review geocoding process for invalid coordinates")
        
        # General recommendations
        if len(errors) > 0:
            recommendations.append("Implement automated data quality monitoring")
        
        if len(warnings) > 0:
            recommendations.append("Set up alerting for data quality warnings")
        
        return recommendations
