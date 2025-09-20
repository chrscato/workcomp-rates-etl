# MRF ETL Partitioned Data Dictionary

## Overview

This document describes the data schema and structure for the MRF (Machine Readable Files) ETL pipeline output optimized for **pre-joined partitioned data**. The data is organized in a denormalized, partitioned structure designed for high-performance analytics and memory-efficient querying.

## Data Architecture

The partitioned ETL pipeline processes healthcare rate and provider data into a **pre-joined, partitioned data warehouse** with the following characteristics:

- **Pre-joined Tables**: Denormalized fact tables with embedded dimension data
- **Partitioned Storage**: Data partitioned by key business dimensions for optimal query performance
- **Memory Efficiency**: Optimized column selection and data types for large-scale analytics
- **Geographic Enrichment**: Enhanced with geolocation data for spatial analysis

## Partitioning Strategy

### Primary Partition Keys
Data is partitioned hierarchically by the following dimensions:

1. **`payer_slug`** - Insurance payer (e.g., `unitedhealthcare-of-georgia-inc`)
2. **`state`** - US state code (e.g., `GA`)
3. **`billing_class`** - Billing type (`professional`, `institutional`)
4. **`procedure_set`** - High-level procedure category (e.g., `Evaluation and Management`)
5. **`procedure_class`** - Procedure class (e.g., `Office/outpatient services`)
6. **`taxonomy`** - Provider specialty taxonomy (e.g., `101YP2500X`)
7. **`stat_area_name`** - Statistical area name (e.g., `Atlanta-Sandy Springs-Alpharetta, GA`)

### Partition Structure
```
data/partitioned/
├── payer=unitedhealthcare-of-georgia-inc/
│   ├── state=GA/
│   │   ├── billing_class=professional/
│   │   │   ├── procedure_set=Evaluation and Management/
│   │   │   │   ├── procedure_class=Office/outpatient services/
│   │   │   │   │   ├── taxonomy=101YP2500X/
│   │   │   │   │   │   ├── stat_area_name=Atlanta-Sandy Springs-Alpharetta, GA/
│   │   │   │   │   │   │   └── fact_rate_enriched.parquet
│   │   │   │   │   │   └── stat_area_name=Augusta-Richmond County, GA-SC/
│   │   │   │   │   │       └── fact_rate_enriched.parquet
│   │   │   │   │   └── taxonomy=101Y00000X/
│   │   │   │   └── procedure_class=Emergency department services/
│   │   │   └── procedure_set=Surgery/
│   │   └── billing_class=institutional/
│   └── state=FL/
└── payer=anthem-blue-cross-blue-shield/
```

## Core Tables

### Pre-Joined Fact Table

#### `fact_rate_enriched.parquet` (Partitioned)
**Purpose**: Denormalized fact table containing negotiated healthcare rates with embedded dimension data

#### Core Rate Fields
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `fact_uid` | String | Unique identifier for each rate record | `2c6a6a035d7ab8b5558f9b422ace9a32` |
| `state` | String | US state code | `GA` |
| `year_month` | String | Rate effective period (YYYY-MM) | `2025-08` |
| `payer_slug` | String | Normalized payer identifier | `unitedhealthcare-of-georgia-inc` |
| `billing_class` | String | Type of billing | `professional` |
| `code_type` | String | Code system (CPT, HCPCS, etc.) | `CPT` |
| `code` | String | Medical procedure code | `33216` |
| `negotiated_type` | String | Rate type | `negotiated` |
| `negotiation_arrangement` | String | Contract arrangement type | `ffs` |
| `negotiated_rate` | Float64 | **The negotiated rate amount** | `752.23` |
| `expiration_date` | String | Rate expiration date | `9999-12-31` |

#### Provider Group Fields (from dim_provider_group)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `provider_group_id_raw` | Int64 | Original provider group ID | `772` |
| `reporting_entity_name` | String | Full payer name | `UnitedHealthcare of Georgia Inc.` |

#### Procedure Code Fields (from dim_code)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `code_description` | String | Full procedure description | `Tracheostoma revision; complex, with flap rotation` |
| `code_name` | String | Shortened procedure name | `TRACHEOSTOMA REVJ CPLX W/FLAP ROTATION` |

#### Procedure Category Fields (from dim_code_cat)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `procedure_set` | String | High-level procedure category | `Evaluation and Management` |
| `procedure_class` | String | Procedure class | `Office/outpatient services` |
| `procedure_group` | String | Specific procedure group | `New office visits` |

#### Place of Service Fields (from dim_pos_set)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pos_set_id` | String | Place of service set identifier | `17b00c58b3dcdb9c20cb2a70b52a4cc1` |
| `pos_members` | List[String] | List of place of service codes | `["02", "05", "06", "07", "08", "19", "21", "22", "23", "24", "26", "31", "34", "41", "42", "51", "52", "53", "56", "61"]` |

#### Provider Information Fields (from dim_npi)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `npi` | String | National Provider Identifier | `1003070913` |
| `first_name` | String | Provider first name | `STEPHANIE` |
| `last_name` | String | Provider last name | `LEONI` |
| `organization_name` | String | Organization name | `HEADWAY COLORADO BEHAVIORAL HEALTH SERVICES, INC.` |
| `enumeration_type` | String | NPI type (NPI-1 individual, NPI-2 organization) | `NPI-1` |
| `status` | String | Provider status | `A` |
| `primary_taxonomy_code` | String | Primary specialty code | `101YP2500X` |
| `primary_taxonomy_desc` | String | Primary specialty description | `Counselor, Professional` |
| `primary_taxonomy_state` | String | License state | `GA` |
| `primary_taxonomy_license` | String | License number | `LPC005043` |
| `credential` | String | Professional credential | `LPC` |
| `sole_proprietor` | String | Sole proprietor status | `YES` |
| `enumeration_date` | String | NPI enumeration date | `2008-07-10` |
| `last_updated` | String | Last update date | `2021-09-03` |

#### Provider Address Fields (from dim_npi_address)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `address_purpose` | String | Address type (MAILING/LOCATION) | `MAILING` |
| `address_type` | String | Address format | `DOM` |
| `address_1` | String | Primary address line | `582 MOUNT GERIZIM RD SE` |
| `address_2` | String | Secondary address line | `BLDG 400, STE 102` |
| `city` | String | City | `MABLETON` |
| `state` | String | State code | `GA` |
| `postal_code` | String | ZIP code | `301266410` |
| `country_code` | String | Country code | `US` |
| `telephone_number` | String | Phone number | `4047301650` |
| `fax_number` | String | Fax number | `7062868442` |
| `address_hash` | String | Unique address identifier | `cd0237207cae95b80fa11879df9fb182` |

#### Geolocation Fields (from dim_npi_address_geolocation)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `latitude` | Float64 | Geographic latitude | `33.7748` |
| `longitude` | Float64 | Geographic longitude | `-84.2963` |
| `county_name` | String | County name from geocoding | `Fulton County` |
| `county_fips` | String | County FIPS code | `13121` |
| `stat_area_name` | String | Statistical area name | `Atlanta-Sandy Springs-Alpharetta, GA` |
| `stat_area_code` | String | CBSA or CSA code | `12060` |
| `matched_address` | String | Geocoded address that was matched | `582 Mount Gerizim Rd SE, Mableton, GA, 30126` |

#### Medicare Benchmark Fields (from bench_medicare_comprehensive)
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `benchmark_type` | String | Benchmark type (asc/opps/professional) | `professional` |
| `medicare_national_rate` | Float64 | Medicare national rate | `128.93` |
| `medicare_state_rate` | Float64 | Medicare state average rate | `128.16` |
| `work_rvu` | Float64 | Work RVU (professional only) | `1.23` |
| `practice_expense_rvu` | Float64 | Practice Expense RVU (professional only) | `0.45` |
| `malpractice_rvu` | Float64 | Malpractice RVU (professional only) | `0.12` |
| `total_rvu` | Float64 | Total RVU (professional only) | `1.80` |
| `conversion_factor` | Float64 | Medicare conversion factor (professional only) | `32.7442` |
| `opps_weight` | Float64 | OPPS relative weight (OPPS only) | `1.25` |
| `opps_si` | String | OPPS status indicator (OPPS only) | `E1` |
| `asc_pi` | String | ASC payment indicator (ASC only) | `R2` |

#### Calculated Fields
| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `rate_to_medicare_ratio` | Float64 | negotiated_rate / medicare_state_rate | `5.87` |
| `is_above_medicare` | Boolean | negotiated_rate > medicare_state_rate | `true` |
| `provider_type` | String | Individual vs Organization | `Individual` |
| `is_sole_proprietor` | Boolean | Derived from sole_proprietor field | `true` |

## Data Relationships

### Pre-Joined Relationships
All dimension data is embedded in the fact table, eliminating the need for runtime joins:

- **Provider Group**: `pg_uid` → embedded provider group data
- **Procedure Codes**: `code_type,code` → embedded code and category data
- **Provider Info**: `npi` → embedded provider and address data
- **Geolocation**: `address_hash` → embedded geolocation data
- **Medicare Benchmarks**: `state,code,code_type` → embedded benchmark data

### Cross-Reference Data
Provider group relationships are maintained through the embedded NPI and TIN data:

- **Provider Groups → NPIs**: Embedded in provider information
- **Provider Groups → TINs**: Available through provider group data

## Memory Optimization Strategy

### Column Selection
Only essential columns are included in the pre-joined table:

**Included Fields:**
- All fact table fields (rate data)
- Key dimension fields for analysis
- Geographic fields for spatial queries
- Medicare benchmark fields for comparison
- Calculated fields for common analytics

**Excluded Fields:**
- Redundant identifiers (kept only primary keys)
- Large text fields not used in analytics
- Historical/audit fields
- Internal processing fields

### Data Type Optimization
- **Strings**: Categorical data converted to categories where possible
- **Floats**: Appropriate precision for rate calculations
- **Booleans**: Converted from string representations
- **Lists**: Optimized for parquet storage

### Partitioning Benefits
- **Query Performance**: Only relevant partitions are read
- **Memory Efficiency**: Smaller data chunks per query
- **Parallel Processing**: Independent partition processing
- **Storage Optimization**: Better compression within partitions

## Usage Examples

### Basic Rate Query (No Joins Required)
```sql
SELECT 
    negotiated_rate,
    code_description,
    reporting_entity_name,
    provider_group_id_raw,
    procedure_set,
    procedure_class,
    stat_area_name
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND code = '99213'
  AND negotiated_rate > 100
```

### Geographic Analysis
```sql
SELECT 
    stat_area_name,
    county_name,
    procedure_set,
    AVG(negotiated_rate) as avg_rate,
    COUNT(*) as rate_count
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND stat_area_name IS NOT NULL
GROUP BY stat_area_name, county_name, procedure_set
ORDER BY avg_rate DESC
```

### Medicare Benchmark Analysis
```sql
SELECT 
    code,
    code_description,
    negotiated_rate,
    medicare_state_rate,
    rate_to_medicare_ratio,
    benchmark_type
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND negotiated_rate > 0
  AND medicare_state_rate > 0
  AND rate_to_medicare_ratio > 2.0
ORDER BY rate_to_medicare_ratio DESC
```

### Provider Network Analysis
```sql
SELECT 
    provider_group_id_raw,
    primary_taxonomy_desc,
    stat_area_name,
    COUNT(DISTINCT npi) as provider_count,
    AVG(negotiated_rate) as avg_rate
FROM fact_rate_enriched
WHERE state = 'GA'
  AND enumeration_type = 'NPI-1'
GROUP BY provider_group_id_raw, primary_taxonomy_desc, stat_area_name
ORDER BY provider_count DESC
```

### Spatial Analysis
```sql
SELECT 
    latitude,
    longitude,
    county_name,
    stat_area_name,
    procedure_set,
    AVG(negotiated_rate) as avg_rate
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
GROUP BY latitude, longitude, county_name, stat_area_name, procedure_set
```

## Implementation Guidelines

### Partition Creation Strategy
1. **Hierarchical Partitioning**: Create partitions in order of selectivity
2. **Memory Management**: Process one partition at a time
3. **Parallel Processing**: Use multiple workers for independent partitions
4. **Incremental Updates**: Support adding new partitions without reprocessing

### Data Quality Considerations
- **Null Handling**: Consistent null handling across all embedded fields
- **Data Validation**: Validate all embedded relationships
- **Consistency Checks**: Ensure data consistency within partitions
- **Error Handling**: Robust error handling for geocoding failures

### Performance Optimization
- **Column Pruning**: Only read required columns
- **Predicate Pushdown**: Leverage partition pruning
- **Compression**: Use appropriate compression for each data type
- **Indexing**: Consider secondary indexes for common query patterns

## Data Sources

- **Rates Data**: UnitedHealthcare of Georgia Inc. MRF files
- **Provider Data**: NPPES (National Plan and Provider Enumeration System)
- **Code Data**: CPT/HCPCS procedure codes with categorization
- **Geolocation Data**: US Census Bureau Geocoding API
- **Medicare Benchmark Data**: CMS Medicare payment rates and RVU calculations
- **Geographic Scope**: Georgia (GA) state (rates), All US states (Medicare benchmarks)
- **Time Period**: August 2025 rates, January 2025 Medicare benchmarks

## Technical Notes

- **File Format**: Parquet with ZSTD compression
- **Processing Engine**: Polars with DuckDB for complex operations
- **Partition Strategy**: Hive-style partitioning by business dimensions
- **Update Strategy**: Idempotent partition-level updates
- **Data Versioning**: Version 2.0.0 for partitioned schema
