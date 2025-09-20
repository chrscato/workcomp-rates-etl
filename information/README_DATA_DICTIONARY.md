# MRF ETL Data Dictionary

## Overview

This document describes the data schema and structure for the MRF (Machine Readable Files) ETL pipeline output. The data is organized in a star schema with fact and dimension tables, optimized for healthcare rate analysis and provider network queries.

## Data Architecture

The ETL pipeline processes healthcare rate and provider data into a structured data warehouse with the following layers:

- **Gold Layer**: Fact tables containing the core business metrics
- **Dimensions**: Lookup tables for entities like providers, codes, and payers
- **Cross-References**: Bridge tables linking provider groups to individual providers

## File Structure

```
prod_etl/core/data/
├── gold/
│   └── fact_rate.parquet          # Main fact table (2.97M rows)
├── dims/
│   ├── dim_code.parquet           # Medical procedure codes (3,696 rows)
│   ├── dim_code_cat.parquet       # Code categorization (8,992 rows)
│   ├── dim_npi.parquet            # Provider information (1,214 rows)
│   ├── dim_npi_address.parquet    # Provider addresses (2,428 rows)
│   ├── dim_payer.parquet          # Insurance payer info (1 row)
│   ├── dim_pos_set.parquet        # Place of service sets (4 rows)
│   ├── dim_provider_group.parquet # Provider groups (637 rows)
│   ├── bench_medicare_asc.parquet # Medicare ASC benchmarks (218,608 rows)
│   ├── bench_medicare_comprehensive.parquet # Combined Medicare benchmarks (1,789,364 rows)
│   ├── bench_medicare_opps.parquet # Medicare OPPS benchmarks (971,464 rows)
│   └── bench_medicare_professional.parquet # Medicare professional benchmarks (599,292 rows)
└── xrefs/
    ├── xref_pg_member_npi.parquet # Provider group → NPI mapping (16,999 rows)
    └── xref_pg_member_tin.parquet # Provider group → TIN mapping (9,486 rows)
```

## Core Tables

### Fact Table

#### `fact_rate.parquet` (2,967,105 rows)
**Purpose**: Central fact table containing negotiated healthcare rates

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `fact_uid` | String | Unique identifier for each rate record | `2c6a6a035d7ab8b5558f9b422ace9a32` |
| `state` | String | US state code | `GA` |
| `year_month` | String | Rate effective period (YYYY-MM) | `2025-08` |
| `payer_slug` | String | Normalized payer identifier | `unitedhealthcare-of-georgia-inc` |
| `billing_class` | String | Type of billing (professional/institutional) | `professional` |
| `code_type` | String | Code system (CPT, HCPCS, etc.) | `CPT` |
| `code` | String | Medical procedure code | `33216` |
| `pg_uid` | String | Provider group unique identifier | `049049fa50d881db5db61293fa01cb5e` |
| `pos_set_id` | String | Place of service set identifier | `d41d8cd98f00b204e9800998ecf8427e` |
| `negotiated_type` | String | Rate type (negotiated, fee schedule, etc.) | `negotiated` |
| `negotiation_arrangement` | String | Contract arrangement type | `ffs` |
| `negotiated_rate` | Float64 | **The negotiated rate amount** | `752.23` |
| `expiration_date` | String | Rate expiration date | `9999-12-31` |
| `provider_group_id_raw` | String | Original provider group ID | `222` |
| `reporting_entity_name` | String | Full payer name | `UnitedHealthcare of Georgia Inc.` |

### Dimension Tables

#### `dim_payer.parquet` (1 row)
**Purpose**: Insurance payer information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `payer_slug` | String | Normalized payer identifier | `unitedhealthcare-of-georgia-inc` |
| `reporting_entity_name` | String | Full payer name | `UnitedHealthcare of Georgia Inc.` |
| `version` | String | Data version | `1.0.0` |

#### `dim_code.parquet` (3,696 rows)
**Purpose**: Medical procedure codes and descriptions

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `code_type` | String | Code system type | `CPT` |
| `code` | String | Procedure code | `31614` |
| `code_description` | String | Full procedure description | `Tracheostoma revision; complex, with flap rotation` |
| `code_name` | String | Shortened procedure name | `TRACHEOSTOMA REVJ CPLX W/FLAP ROTATION` |

#### `dim_code_cat.parquet` (8,992 rows)
**Purpose**: Procedure code categorization and grouping

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `proc_cd` | String | Procedure code | `99201` |
| `proc_set` | String | High-level procedure category | `Evaluation and Management` |
| `proc_class` | String | Procedure class | `Office/ outpatient services` |
| `proc_group` | String | Specific procedure group | `New office visits` |

#### `dim_provider_group.parquet` (637 rows)
**Purpose**: Provider group information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pg_uid` | String | Provider group unique identifier | `b8b29688e92394ea7cc3d736446337d0` |
| `payer_slug` | String | Associated payer | `unitedhealthcare-of-georgia-inc` |
| `provider_group_id_raw` | Int64 | Original provider group ID | `772` |
| `version` | String | Data version | `1.0.0` |

#### `dim_pos_set.parquet` (4 rows)
**Purpose**: Place of service groupings

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pos_set_id` | String | Place of service set identifier | `17b00c58b3dcdb9c20cb2a70b52a4cc1` |
| `pos_members` | List[String] | List of place of service codes | `["02", "05", "06", "07", "08", "19", "21", "22", "23", "24", "26", "31", "34", "41", "42", "51", "52", "53", "56", "61"]` |

#### `dim_npi.parquet` (1,214 rows)
**Purpose**: Individual provider (NPI) information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `npi` | String | National Provider Identifier | `1003070913` |
| `first_name` | String | Provider first name (23.48% null) | `STEPHANIE` |
| `last_name` | String | Provider last name (23.48% null) | `LEONI` |
| `organization_name` | String | Organization name (76.52% null) | `HEADWAY COLORADO BEHAVIORAL HEALTH SERVICES, INC.` |
| `enumeration_type` | String | NPI type (NPI-1 individual, NPI-2 organization) | `NPI-1` |
| `status` | String | Provider status | `A` |
| `primary_taxonomy_code` | String | Primary specialty code | `101YP2500X` |
| `primary_taxonomy_desc` | String | Primary specialty description | `Counselor, Professional` |
| `primary_taxonomy_state` | String | License state (19.36% null) | `GA` |
| `primary_taxonomy_license` | String | License number (20.35% null) | `LPC005043` |
| `credential` | String | Professional credential (31.47% null) | `LPC` |
| `sole_proprietor` | String | Sole proprietor status (23.48% null) | `YES` |
| `enumeration_date` | String | NPI enumeration date | `2008-07-10` |
| `last_updated` | String | Last update date | `2021-09-03` |
| `nppes_fetched` | Boolean | Whether NPPES data was fetched | `True` |
| `nppes_fetch_date` | String | NPPES fetch date | `2021-09-03` |
| `replacement_npi` | String | Replacement NPI (100% null) | `null` |

#### `dim_npi_address.parquet` (2,428 rows)
**Purpose**: Provider address information

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `npi` | String | National Provider Identifier | `1235233776` |
| `address_purpose` | String | Address type (MAILING/LOCATION) | `MAILING` |
| `address_type` | String | Address format | `DOM` |
| `address_1` | String | Primary address line | `582 MOUNT GERIZIM RD SE` |
| `address_2` | String | Secondary address line (78.42% null) | `BLDG 400, STE 102` |
| `city` | String | City | `MABLETON` |
| `state` | String | State code | `GA` |
| `postal_code` | String | ZIP code | `301266410` |
| `country_code` | String | Country code | `US` |
| `telephone_number` | String | Phone number (5.19% null) | `4047301650` |
| `fax_number` | String | Fax number (45.35% null) | `7062868442` |
| `last_updated` | String | Last update date | `2007-07-08` |
| `address_hash` | String | Unique address identifier | `cd0237207cae95b80fa11879df9fb182` |

### Medicare Benchmark Tables

#### `bench_medicare_asc.parquet` (218,608 rows)
**Purpose**: Medicare Ambulatory Surgical Center (ASC) payment rates and benchmarks

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `state` | String | US state code | `AK` |
| `year_month` | String | Rate effective period (YYYY-MM) | `2025-01` |
| `code_type` | String | Code system (CPT, HCPCS) | `CPT` |
| `code` | String | Medical procedure code | `0101T` |
| `asc_pi` | String | ASC payment indicator | `R2` |
| `asc_nat_rate` | Float64 | National ASC rate | `128.93` |
| `asc_short_desc` | String | Short procedure description | `Esw muscskel sys nos` |
| `state_wage_index_avg` | Float64 | State wage index average | `0.988` |
| `medicare_asc_national` | Float64 | Medicare ASC national rate | `128.93` |
| `asc_adj_factor_stateavg` | Float64 | ASC adjustment factor for state | `0.994` |
| `medicare_asc_stateavg` | Float64 | Medicare ASC state average rate | `128.16` |
| `benchmark_type` | String | Benchmark type identifier | `asc` |
| `created_date` | Datetime | Record creation timestamp | `2025-09-10 09:10:25` |
| `data_year` | Int64 | Data year | `2025` |

#### `bench_medicare_opps.parquet` (971,464 rows)
**Purpose**: Medicare Outpatient Prospective Payment System (OPPS) rates and benchmarks

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `state` | String | US state code | `AK` |
| `year_month` | String | Rate effective period (YYYY-MM) | `2025-01` |
| `code_type` | String | Code system (CPT, HCPCS) | `HCPCS` |
| `code` | String | Medical procedure code | `0001F` |
| `opps_weight` | Float64 | OPPS relative weight | `NaN` |
| `opps_si` | String | OPPS status indicator | `E1` |
| `opps_short_desc` | String | Short procedure description | `Heart failure composite` |
| `state_wage_index_avg` | Float64 | State wage index average | `0.988` |
| `medicare_opps_national` | Float64 | Medicare OPPS national rate | `NaN` |
| `opps_adj_factor_stateavg` | Float64 | OPPS adjustment factor for state | `0.9928` |
| `medicare_opps_stateavg` | Float64 | Medicare OPPS state average rate | `NaN` |
| `benchmark_type` | String | Benchmark type identifier | `opps` |
| `created_date` | Datetime | Record creation timestamp | `2025-09-10 09:10:23` |
| `data_year` | Int64 | Data year | `2025` |

#### `bench_medicare_professional.parquet` (599,292 rows)
**Purpose**: Medicare Professional services rates and RVU-based calculations

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `state` | String | US state code | `AL` |
| `year_month` | String | Rate effective period (YYYY-MM) | `2025-01` |
| `code_type` | String | Code system (CPT, HCPCS) | `CPT` |
| `code` | String | Medical procedure code | `A0021` |
| `modifier` | Null | Procedure modifier (null in this dataset) | `null` |
| `work_rvu` | Float64 | Work Relative Value Unit | `0.0` |
| `practice_expense_rvu` | Float64 | Practice Expense RVU | `0.0` |
| `malpractice_rvu` | Float64 | Malpractice RVU | `0.0` |
| `total_rvu` | Float64 | Total RVU | `0.0` |
| `year` | Int64 | Payment year | `2025` |
| `work_gpci` | Float64 | Work Geographic Practice Cost Index | `1.023722` |
| `pe_gpci` | Float64 | Practice Expense GPCI | `0.9995` |
| `mp_gpci` | Float64 | Malpractice GPCI | `0.842083` |
| `medicare_prof_national` | Float64 | Medicare professional national rate | `0.0` |
| `medicare_prof_stateavg` | Float64 | Medicare professional state average | `0.0` |
| `conversion_factor` | Float64 | Medicare conversion factor | `32.7442` |
| `benchmark_type` | String | Benchmark type identifier | `professional` |
| `created_date` | Datetime | Record creation timestamp | `2025-09-10 09:10:20` |
| `data_year` | Int64 | Data year | `2025` |

#### `bench_medicare_comprehensive.parquet` (1,789,364 rows)
**Purpose**: Combined Medicare benchmark data containing all ASC, OPPS, and Professional rates

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| `state` | String | US state code | Join key |
| `year_month` | String | Rate effective period | Join key |
| `code_type` | String | Code system | Join key |
| `code` | String | Medical procedure code | Join key |
| `modifier` | Null | Procedure modifier | Usually null |
| `work_rvu` | Float64 | Work RVU | Professional services only |
| `practice_expense_rvu` | Float64 | Practice Expense RVU | Professional services only |
| `malpractice_rvu` | Float64 | Malpractice RVU | Professional services only |
| `total_rvu` | Float64 | Total RVU | Professional services only |
| `year` | Float64 | Payment year | Professional services only |
| `work_gpci` | Float64 | Work GPCI | Professional services only |
| `pe_gpci` | Float64 | Practice Expense GPCI | Professional services only |
| `mp_gpci` | Float64 | Malpractice GPCI | Professional services only |
| `medicare_prof_national` | Float64 | Professional national rate | Professional services only |
| `medicare_prof_stateavg` | Float64 | Professional state average | Professional services only |
| `conversion_factor` | Float64 | Medicare conversion factor | Professional services only |
| `benchmark_type` | String | Benchmark type (asc/opps/professional) | Identifies rate type |
| `created_date` | Datetime | Record creation timestamp | |
| `data_year` | Int64 | Data year | |
| `opps_weight` | Float64 | OPPS relative weight | OPPS services only |
| `opps_si` | String | OPPS status indicator | OPPS services only |
| `opps_short_desc` | String | OPPS procedure description | OPPS services only |
| `state_wage_index_avg` | Float64 | State wage index average | All services |
| `medicare_opps_national` | Float64 | OPPS national rate | OPPS services only |
| `opps_adj_factor_stateavg` | Float64 | OPPS state adjustment factor | OPPS services only |
| `medicare_opps_stateavg` | Float64 | OPPS state average rate | OPPS services only |
| `asc_pi` | String | ASC payment indicator | ASC services only |
| `asc_nat_rate` | Float64 | ASC national rate | ASC services only |
| `asc_short_desc` | String | ASC procedure description | ASC services only |
| `medicare_asc_national` | Float64 | ASC national rate | ASC services only |
| `asc_adj_factor_stateavg` | Float64 | ASC state adjustment factor | ASC services only |
| `medicare_asc_stateavg` | Float64 | ASC state average rate | ASC services only |
| `table_version` | String | Table version | `1.0` |
| `last_updated` | Datetime | Last update timestamp | |

### Cross-Reference Tables

#### `xref_pg_member_npi.parquet` (16,999 rows)
**Purpose**: Maps provider groups to individual NPIs

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pg_uid` | String | Provider group unique identifier | `33c5ebf41b7fe9461b8ccf3202cb6604` |
| `npi` | String | National Provider Identifier | `1780875781` |

#### `xref_pg_member_tin.parquet` (9,486 rows)
**Purpose**: Maps provider groups to Tax Identification Numbers

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| `pg_uid` | String | Provider group unique identifier | `11ce3cbdcf491bc5ea76386e84a55b4d` |
| `tin_type` | String | TIN type | `ein` |
| `tin_value` | String | Tax ID number | `881009565` |

## Data Relationships

### Primary Relationships
- **Fact → Dimensions**: All foreign keys validated with 100% integrity
  - `fact_rate.payer_slug` → `dim_payer.payer_slug`
  - `fact_rate.code_type,code` → `dim_code.code_type,code`
  - `fact_rate.pg_uid` → `dim_provider_group.pg_uid`
  - `fact_rate.pos_set_id` → `dim_pos_set.pos_set_id`

### Medicare Benchmark Relationships
- **Fact → Medicare Benchmarks**: Join on state, code, and optionally code_type
  - `fact_rate.state,code` → `bench_medicare_*.state,code`
  - `fact_rate.state,code,code_type` → `bench_medicare_*.state,code,code_type` (recommended)
  - **Note**: Some codes may appear in multiple benchmark types (ASC, OPPS, Professional)

### Cross-Reference Relationships
- **Provider Groups → NPIs**: 637 provider groups mapped to 15,885 unique NPIs
- **Provider Groups → TINs**: 637 provider groups mapped to 8,188 unique TINs

## Key Identifiers

### `pg_uid` (Provider Group UID)
- **Format**: MD5 hash of `payer_slug|version|provider_group_id|""`
- **Purpose**: Unique identifier for provider groups across all tables
- **Usage**: Links fact table to provider group dimensions and cross-references

### `fact_uid` (Fact UID)
- **Format**: MD5 hash of all fact dimensions plus rate amount
- **Purpose**: Unique identifier for each rate record
- **Usage**: Enables idempotent upserts and deduplication

### `pos_set_id` (Place of Service Set ID)
- **Format**: MD5 hash of normalized place of service code list
- **Purpose**: Groups related place of service codes
- **Usage**: Simplifies place of service filtering and analysis

## Data Quality

### Completeness
- **Fact Table**: 0% nulls in key fields
- **Dimensions**: High completeness (>95%) for critical fields
- **Cross-References**: 100% complete for linking fields

### Uniqueness
- **Fact UIDs**: 100% unique (2,967,105 unique values)
- **Dimension Keys**: 100% unique across all dimension tables
- **Provider Groups**: 637 unique groups with complete NPI/TIN mappings

### Referential Integrity
- **All foreign keys validated**: 0 orphaned records
- **Cross-reference integrity**: All provider groups in fact table have NPI/TIN mappings

## Usage Examples

### Basic Rate Query
```sql
SELECT 
    f.negotiated_rate,
    c.code_description,
    p.reporting_entity_name,
    pg.provider_group_id_raw
FROM fact_rate f
JOIN dim_code c ON f.code_type = c.code_type AND f.code = c.code
JOIN dim_payer p ON f.payer_slug = p.payer_slug
JOIN dim_provider_group pg ON f.pg_uid = pg.pg_uid
WHERE f.state = 'GA' 
  AND f.code = '99213'
  AND f.negotiated_rate > 100
```

### Provider Network Analysis
```sql
SELECT 
    pg.provider_group_id_raw,
    COUNT(DISTINCT npi.npi) as provider_count,
    COUNT(DISTINCT tin.tin_value) as tin_count
FROM dim_provider_group pg
LEFT JOIN xref_pg_member_npi npi ON pg.pg_uid = npi.pg_uid
LEFT JOIN xref_pg_member_tin tin ON pg.pg_uid = tin.pg_uid
GROUP BY pg.provider_group_id_raw
```

### Rate Analysis by Procedure Category
```sql
SELECT 
    cat.proc_set,
    cat.proc_class,
    AVG(f.negotiated_rate) as avg_rate,
    COUNT(*) as rate_count
FROM fact_rate f
JOIN dim_code_cat cat ON f.code = cat.proc_cd
GROUP BY cat.proc_set, cat.proc_class
ORDER BY avg_rate DESC
```

### Medicare Benchmark Analysis
```sql
-- Compare negotiated rates to Medicare ASC benchmarks
SELECT 
    f.state,
    f.code,
    f.code_type,
    f.negotiated_rate,
    asc.medicare_asc_stateavg as medicare_asc_rate,
    (f.negotiated_rate / asc.medicare_asc_stateavg) as rate_ratio,
    asc.asc_short_desc
FROM fact_rate f
JOIN bench_medicare_asc asc ON f.state = asc.state 
    AND f.code = asc.code 
    AND f.code_type = asc.code_type
WHERE f.state = 'GA' 
  AND f.negotiated_rate > 0
  AND asc.medicare_asc_stateavg > 0
ORDER BY rate_ratio DESC
```

### Comprehensive Medicare Benchmark Comparison
```sql
-- Compare rates across all Medicare benchmark types
SELECT 
    f.state,
    f.code,
    f.code_type,
    f.negotiated_rate,
    comp.benchmark_type,
    CASE 
        WHEN comp.benchmark_type = 'asc' THEN comp.medicare_asc_stateavg
        WHEN comp.benchmark_type = 'opps' THEN comp.medicare_opps_stateavg
        WHEN comp.benchmark_type = 'professional' THEN comp.medicare_prof_stateavg
    END as medicare_rate,
    CASE 
        WHEN comp.benchmark_type = 'asc' THEN (f.negotiated_rate / comp.medicare_asc_stateavg)
        WHEN comp.benchmark_type = 'opps' THEN (f.negotiated_rate / comp.medicare_opps_stateavg)
        WHEN comp.benchmark_type = 'professional' THEN (f.negotiated_rate / comp.medicare_prof_stateavg)
    END as rate_ratio
FROM fact_rate f
JOIN bench_medicare_comprehensive comp ON f.state = comp.state 
    AND f.code = comp.code 
    AND f.code_type = comp.code_type
WHERE f.state = 'GA' 
  AND f.negotiated_rate > 0
  AND comp.benchmark_type IS NOT NULL
ORDER BY rate_ratio DESC
```

### Professional Services RVU Analysis
```sql
-- Analyze professional services with RVU data
SELECT 
    f.state,
    f.code,
    f.negotiated_rate,
    prof.work_rvu,
    prof.practice_expense_rvu,
    prof.malpractice_rvu,
    prof.total_rvu,
    prof.conversion_factor,
    prof.medicare_prof_stateavg,
    (f.negotiated_rate / prof.medicare_prof_stateavg) as rate_ratio
FROM fact_rate f
JOIN bench_medicare_professional prof ON f.state = prof.state 
    AND f.code = prof.code 
    AND f.code_type = prof.code_type
WHERE f.state = 'GA' 
  AND f.negotiated_rate > 0
  AND prof.total_rvu > 0
  AND prof.medicare_prof_stateavg > 0
ORDER BY prof.total_rvu DESC
```

## Data Sources

- **Rates Data**: UnitedHealthcare of Georgia Inc. MRF files
- **Provider Data**: NPPES (National Plan and Provider Enumeration System)
- **Code Data**: CPT/HCPCS procedure codes with categorization
- **Medicare Benchmark Data**: CMS Medicare payment rates and RVU calculations
  - ASC (Ambulatory Surgical Center) rates
  - OPPS (Outpatient Prospective Payment System) rates
  - Professional services with RVU-based calculations
- **Geographic Scope**: Georgia (GA) state (rates), All US states (Medicare benchmarks)
- **Time Period**: August 2025 rates, January 2025 Medicare benchmarks

## Technical Notes

- **File Format**: Parquet with ZSTD compression
- **Processing Engine**: Polars with DuckDB for complex operations
- **Update Strategy**: Idempotent upserts based on unique identifiers
- **Data Versioning**: Version 1.0.0 across all dimensions
