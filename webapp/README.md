# 🏥 Healthcare Partition Navigator Webapp

A Streamlit-based web application for navigating and exploring S3-partitioned healthcare data using the partition navigation database.

## Features

### 🔍 Advanced Search & Filtering
- Filter partitions by payer, state, medical specialty, billing class, procedure set, and more
- Real-time search with instant results
- Hierarchical filtering for precise data discovery

### 📊 Analytics Dashboard
- Overview metrics and statistics
- Top states by partition count
- Medical specialty distribution
- File size and temporal analysis

### 📈 Interactive Visualizations
- Interactive charts using Plotly
- File size distribution analysis
- Temporal trends over time
- Pie charts and bar graphs for data exploration

### 👁️ Data Preview
- Preview partition contents directly from S3
- Download preview data as CSV
- Real-time data sampling

## Quick Start

### Prerequisites
- Python 3.8+
- AWS credentials configured (for S3 access)
- Partition navigation database (created by `s3_partition_inventory.py`)

### Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the application:**
   ```bash
   python run_app.py
   ```
   
   Or directly with Streamlit:
   ```bash
   streamlit run app.py
   ```

3. **Open your browser:**
   The app will automatically open at `http://localhost:8501`

## Usage

### 1. Database Selection
- The app automatically detects available partition navigation databases
- Select the database you want to explore from the sidebar

### 2. Search Partitions
- Use the filter form to narrow down your search criteria
- Available filters:
  - **Payer**: Insurance company
  - **State**: US state code
  - **Billing Class**: Institutional vs Professional
  - **Procedure Set**: Medical procedure categories
  - **Taxonomy**: Medical specialties
  - **Statistical Area**: Geographic areas
  - **Year/Month**: Time period

### 3. Preview Data
- Click "Preview Data" on any partition to see a sample of the contents
- Download preview data as CSV for further analysis

### 4. Analytics
- Explore summary statistics in the Analytics tab
- View distributions and trends in the Visualizations tab

## Database Schema

The webapp works with the following database tables:

- **`partitions`**: Main table with partition metadata
- **`dim_payers`**: Payer information
- **`dim_states`**: State information
- **`dim_taxonomies`**: Medical specialty codes and descriptions
- **`dim_billing_classes`**: Billing class categories
- **`dim_procedure_sets`**: Procedure set categories
- **`dim_stat_areas`**: Statistical area information
- **`dim_time_periods`**: Time period information

## Views

- **`v_partition_navigation`**: Main navigation view with all dimensions
- **`v_partition_summary`**: Summary by payer/state/billing class
- **`v_taxonomy_summary`**: Taxonomy usage summary

## Configuration

### AWS Credentials
The app requires AWS credentials to access S3 data. Configure them using:
- AWS CLI: `aws configure`
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- IAM roles (if running on EC2)

### Database Location
The app automatically searches for partition navigation databases in:
- Current directory
- Parent directories
- Subdirectories

## Troubleshooting

### Common Issues

1. **"No partition navigation database found"**
   - Run `s3_partition_inventory.py` first to create the database
   - Ensure the database file is in the expected location

2. **"Error connecting to database"**
   - Check that the database file exists and is readable
   - Verify the database was created successfully

3. **"Error reading partition preview"**
   - Verify AWS credentials are configured
   - Check that the S3 bucket and key are accessible
   - Ensure the partition file exists in S3

4. **Streamlit not found**
   - Install requirements: `pip install -r requirements.txt`
   - Or install Streamlit directly: `pip install streamlit`

### Performance Tips

- Use specific filters to reduce search results
- The app limits results to 1000 partitions for performance
- Large partition previews may take time to load

## Development

### Project Structure
```
webapp/
├── app.py              # Main Streamlit application
├── run_app.py          # Launcher script
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

### Adding New Features

1. **New Filters**: Add to the `get_filter_options()` method
2. **New Visualizations**: Add to the Visualizations tab
3. **New Analytics**: Add to the Analytics tab
4. **Database Queries**: Add methods to the `PartitionNavigator` class

## License

This project is part of the BeaconPoint Health WorkComp Rates ETL system.
