# WorkComp Rates ETL

A comprehensive ETL pipeline for processing Workers' Compensation healthcare rate data, designed to power transparency and steerage platforms for the healthcare industry.

## 🏥 Project Overview

**BeaconPoint Health** is building a data-first transparency and steerage platform designed to outcompete incumbents like Turquoise Health in the **Workers' Compensation niche**. This repository contains the ETL infrastructure that processes and transforms healthcare rate data into a queryable, partitioned data warehouse.

### Key Value Propositions
- **Multi-benchmark Integration**: Combines Commercial MRF negotiated rates, State Workers' Comp Fee Schedules, and Medicare benchmarks (ASC, OPPS, Professional)
- **Scalable Architecture**: Iceberg star schema with fact and dimension tables, optimized for both self-service dashboards and steerage guidance
- **Workers' Comp Focus**: Specialized for the underserved Workers' Compensation market
- **Cost-Effective**: Designed for low-cost transparency dashboards and enterprise steerage solutions

## 🏗️ Architecture

### Data Pipeline Stages

#### **Stage 1: Data Infrastructure Foundation** ✅
- **Deliverable**: Iceberg-based fact + dimension tables, partitioned by payer/state/billing class/procedure set
- **ETL**: Automated ingestion of MRF + WC + Medicare benchmarks with idempotent upserts
- **Output**: Pre-cut tiles (Parquet files) partitioned at S3 by payer/state/class/set
- **Stack**: AWS S3 (Parquet/ZSTD), Apache Iceberg, DuckDB/Athena/Trino, Python (Polars, PyArrow)

#### **Stage 2: Commercial Dashboard Offering** 🚧
- **Deliverable**: White-labeled webapp dashboards for payers, TPAs, and provider networks
- **Features**: Hierarchical search, Medicare vs WC FS vs commercial benchmarks, provider network summaries
- **Monetization**: Subscription tiers (Free/Low/Pro/Enterprise)

#### **Stage 3: Steerage Guidance Agent** 🔮
- **Deliverable**: AI-powered decision agent for claims adjustors and risk managers
- **Workflow**: Claim details → LLM prediction → Provider network query → Steerage recommendations
- **Monetization**: Premium per-seat SaaS for adjustors and risk managers

## 📊 Data Schema

### Star Schema Design
The data is organized in a robust **Iceberg star schema** with the following structure:

```
data/
├── gold/
│   └── fact_rate.parquet          # Main fact table (2.97M rows)
├── dims/
│   ├── dim_code.parquet           # Medical procedure codes (3,696 rows)
│   ├── dim_npi.parquet            # Provider information (1,214 rows)
│   ├── dim_npi_address.parquet    # Provider addresses (2,428 rows)
│   ├── dim_payer.parquet          # Insurance payer info (1 row)
│   ├── dim_pos_set.parquet        # Place of service sets (4 rows)
│   ├── dim_provider_group.parquet # Provider groups (637 rows)
│   ├── bench_medicare_asc.parquet # Medicare ASC benchmarks (218,608 rows)
│   ├── bench_medicare_opps.parquet # Medicare OPPS benchmarks (971,464 rows)
│   └── bench_medicare_professional.parquet # Medicare professional benchmarks (599,292 rows)
└── xrefs/
    ├── xref_pg_member_npi.parquet # Provider group → NPI mapping (16,999 rows)
    └── xref_pg_member_tin.parquet # Provider group → TIN mapping (9,486 rows)
```

### Key Data Sources
- **Rates Data**: UnitedHealthcare of Georgia Inc. MRF files
- **Provider Data**: NPPES (National Plan and Provider Enumeration System)
- **Code Data**: CPT/HCPCS procedure codes with categorization
- **Medicare Benchmark Data**: CMS Medicare payment rates and RVU calculations
- **Geographic Scope**: Georgia (GA) state (rates), All US states (Medicare benchmarks)
- **Time Period**: August 2025 rates, January 2025 Medicare benchmarks

## 🚀 Quick Start

### Prerequisites
```bash
# Install dependencies
pip install polars boto3 pyyaml tqdm pandas pyarrow

# Set up AWS credentials
aws configure

# Set environment variables
export S3_BUCKET="your-healthcare-data-lake"
export S3_REGION="us-east-1"
```

### Running the ETL Pipeline

#### ETL1: Basic Data Processing
```bash
# Process raw MRF data into star schema
jupyter notebook ETL/ETL_1.ipynb
```

#### ETL2: Provider Data Management
```bash
# Fetch and manage NPPES provider data
jupyter notebook ETL/ETL_2.ipynb
```

#### ETL3: S3 Partitioned Data Warehouse
```bash
# Create partitioned, pre-joined data warehouse
python ETL/scripts/run_etl3.py --environment development --monitor --quality-check
```

### Querying Data

#### Basic Rate Analysis
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
  AND f.negotiated_rate > 100;
```

#### Medicare Benchmark Comparison
```sql
SELECT 
    f.state,
    f.code,
    f.negotiated_rate,
    asc.medicare_asc_stateavg as medicare_asc_rate,
    (f.negotiated_rate / asc.medicare_asc_stateavg) as rate_ratio
FROM fact_rate f
JOIN bench_medicare_asc asc ON f.state = asc.state 
    AND f.code = asc.code 
    AND f.code_type = asc.code_type
WHERE f.state = 'GA' 
  AND f.negotiated_rate > 0
  AND asc.medicare_asc_stateavg > 0
ORDER BY rate_ratio DESC;
```

## 📁 Repository Structure

```
workcomp-rates-etl/
├── ETL/                           # ETL pipeline notebooks and scripts
│   ├── ETL_1.ipynb               # Basic data processing
│   ├── ETL_2.ipynb               # Provider data management
│   ├── ETL_3.ipynb               # S3 partitioned warehouse
│   ├── scripts/
│   │   ├── run_etl3.py           # Pipeline runner
│   │   └── setup_aws_resources.py # AWS setup
│   ├── utils/
│   │   ├── s3_etl_utils.py       # S3 utilities
│   │   ├── data_quality.py       # Data validation
│   │   ├── monitoring.py         # Performance monitoring
│   │   ├── fetch_npi_data.py     # NPPES data fetching
│   │   └── geo.py                # Geocoding utilities
│   └── config/
│       └── etl3_config.yaml      # Pipeline configuration
├── data/                         # Processed data storage
│   ├── gold/                     # Fact tables
│   ├── dims/                     # Dimension tables
│   ├── xrefs/                    # Cross-reference tables
│   └── input/                    # Raw input data
├── information/                  # Documentation
│   ├── business_concept.md       # Business strategy and vision
│   ├── README_DATA_DICTIONARY.md # Complete data schema documentation
│   ├── PARTITIONED_ETL_IMPLEMENTATION_GUIDELINES.md
│   └── S3_PARTITIONED_ETL_GUIDELINES.md
└── notes/                        # Project notes and priorities
    └── states_to_prioritize.txt  # Target states for expansion
```

## 🔧 Technical Features

### ETL Pipeline Capabilities
- **Memory-Efficient Processing**: Streaming data processing with chunked operations
- **Idempotent Upserts**: Safe re-running of ETL processes
- **Data Quality Validation**: Comprehensive validation rules and monitoring
- **Geographic Processing**: H3-based geocoding and spatial analysis
- **Medicare Integration**: Automated Medicare benchmark calculations

### S3 Partitioning Strategy
```
s3://bucket/partitioned-data/
├── payer=unitedhealthcare-of-georgia-inc/
│   ├── state=GA/
│   │   ├── billing_class=professional/
│   │   │   ├── procedure_set=Evaluation and Management/
│   │   │   │   ├── procedure_class=Office/outpatient services/
│   │   │   │   │   ├── taxonomy=101YP2500X/
│   │   │   │   │   │   ├── stat_area_name=Atlanta-Sandy Springs-Alpharetta, GA/
│   │   │   │   │   │   │   ├── year=2025/
│   │   │   │   │   │   │   │   └── month=08/
│   │   │   │   │   │   │   │       └── fact_rate_enriched.parquet
```

### Performance Optimizations
- **Partition Pruning**: Only load relevant data partitions
- **Column Pruning**: Read only required columns
- **Compression**: ZSTD compression for optimal storage
- **Parallel Processing**: Multi-threaded data processing
- **Lazy Evaluation**: Polars lazy evaluation for query optimization

## 📈 Business Impact

### Revenue Streams
1. **Subscription SaaS (Transparency Dashboards)**: $500–2,500/mo
2. **Enterprise Contracts (Steerage Agent)**: $50k–250k/yr
3. **Data Licensing**: De-identified aggregated data sales

### Competitive Advantages
- **Niche Focus**: Specialized Workers' Comp market
- **Multi-benchmark Integration**: WC FS + Medicare + Commercial in one schema
- **Scalable Architecture**: Iceberg + pre-aggregated tiles
- **Dual Product Lines**: Dashboards + steerage AI

### Cost Structure
- **Stage 1**: $200–500/mo (storage + droplet + Athena queries)
- **Stage 2**: $1–3k/mo infra at scale
- **Stage 3**: $5–10k/mo infra (scales with compute + LLM calls)

## 🎯 Target Markets

### Primary Customers
- **Mid-market TPAs** (Third Party Administrators)
- **Risk Managers** at self-insured employers
- **Provider Networks** seeking rate transparency
- **Municipalities** with Workers' Comp programs

### Geographic Focus
- **Current**: Georgia (GA)
- **Next Priority States**: IA, MO, IN, NH, NJ
- **Expansion**: All US states with Workers' Comp programs

## 🔒 Security & Compliance

### Data Protection
- **Encryption**: AES-256 server-side encryption
- **Access Control**: IAM-based permissions
- **Audit Logging**: CloudTrail integration
- **HIPAA Ready**: Healthcare data protection standards

### Compliance Features
- **SOC 2**: Security controls implementation
- **GDPR**: Data privacy compliance
- **Audit Trails**: Complete activity logging
- **Data Retention**: Configurable lifecycle policies

## 📚 Documentation

### Comprehensive Guides
- **[Business Concept](information/business_concept.md)**: Complete business strategy and product roadmap
- **[Data Dictionary](information/README_DATA_DICTIONARY.md)**: Detailed schema documentation with 468 lines of technical specifications
- **[Partitioned ETL Guidelines](information/PARTITIONED_ETL_IMPLEMENTATION_GUIDELINES.md)**: Implementation best practices
- **[S3 ETL Guidelines](information/S3_PARTITIONED_ETL_GUIDELINES.md)**: AWS-specific implementation details

### API Reference
- **ETL Utils**: `ETL/utils/` module documentation
- **Configuration**: `ETL/config/etl3_config.yaml` settings
- **Scripts**: Command-line interface documentation

## 🚀 Getting Started

1. **Clone the repository**:
   ```bash
   git clone https://github.com/chrscato/workcomp-rates-etl.git
   cd workcomp-rates-etl
   ```

2. **Set up environment**:
   ```bash
   pip install -r requirements.txt
   aws configure
   ```

3. **Run ETL1** (Basic processing):
   ```bash
   jupyter notebook ETL/ETL_1.ipynb
   ```

4. **Run ETL2** (Provider data):
   ```bash
   jupyter notebook ETL/ETL_2.ipynb
   ```

5. **Run ETL3** (Partitioned warehouse):
   ```bash
   python ETL/scripts/run_etl3.py --environment development
   ```

## 🤝 Contributing

This repository is part of the BeaconPoint Health platform development. For contributions, please:

1. Review the business concept and technical documentation
2. Follow the established ETL patterns and data quality standards
3. Ensure all data processing maintains HIPAA compliance
4. Test thoroughly with the provided validation tools

## 📞 Support

- **Documentation**: Comprehensive guides in the `information/` directory
- **Monitoring**: CloudWatch dashboards for real-time metrics
- **Logs**: Detailed execution logs in `logs/` directory
- **Issues**: GitHub Issues for bug reports and feature requests

## 📄 License

This project is proprietary to BeaconPoint Health. All rights reserved.

---

**BeaconPoint Health** - Transforming Workers' Compensation through data-driven transparency and intelligent steerage.
