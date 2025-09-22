# ETL1 Scalable Pipeline

A memory-efficient, production-ready version of the ETL_1.ipynb notebook that can handle large files through batched processing and optimized upserts.

## Key Improvements Over Notebook Version

### 🚀 **Scalability**
- **Batched Processing**: Processes data in configurable chunks (default: 50,000 rows)
- **Memory Management**: Configurable memory limits with automatic cleanup
- **Streaming**: Uses Polars lazy evaluation for large file processing
- **Progress Tracking**: Real-time progress updates and logging

### 💾 **Memory Efficiency**
- **DuckDB Upserts**: Uses DuckDB for efficient deduplication without loading entire files
- **Chunked Upserts**: Only processes small batches at a time
- **Garbage Collection**: Explicit memory cleanup between chunks
- **Lazy Loading**: Only loads required columns and rows

### 🛠️ **Production Ready**
- **Error Handling**: Comprehensive error handling and recovery
- **Logging**: Structured logging with configurable levels
- **Configuration**: YAML-based configuration management
- **CLI Interface**: Command-line interface for easy automation
- **Testing**: Comprehensive test suite for validation

## Quick Start

### 1. Basic Usage

```bash
# Run with default settings
python ETL/scripts/run_etl1_scalable.py --payer aetna

# Run with custom chunk size
python ETL/scripts/run_etl1_scalable.py --payer aetna --chunk-size 100000

# Run with configuration file
python ETL/scripts/run_etl1_scalable.py --payer aetna --config ETL/config/etl1_config.yaml
```

### 2. Advanced Usage

```bash
# High-memory processing
python ETL/scripts/run_etl1_scalable.py \
  --payer aetna \
  --chunk-size 200000 \
  --memory-limit 16384 \
  --verbose

# Different state and payer
python ETL/scripts/run_etl1_scalable.py \
  --payer uhc \
  --state VA \
  --payer-slug unitedhealthcare-virginia

# Dry run to check configuration
python ETL/scripts/run_etl1_scalable.py --payer aetna --dry-run
```

### 3. Programmatic Usage

```python
from ETL.etl1_scalable import ScalableETL1, ETL1Config

# Create configuration
config = ETL1Config()
config.chunk_size = 100000
config.memory_limit_mb = 16384

# Run pipeline
with ScalableETL1(config) as etl:
    summary = etl.run_pipeline("aetna")
    print(f"Processed {summary['processed_rates']:,} rates")
```

## Configuration

### YAML Configuration File

```yaml
# ETL1 Scalable Pipeline Configuration
data_root: "data"
chunk_size: 50000
memory_limit_mb: 8192
state: "GA"
payer_slug_override: null

# File patterns
rates_file_pattern: "202508_{payer}_ga_rates.parquet"
providers_file_pattern: "202508_{payer}_ga_providers.parquet"

# Logging
log_level: "INFO"
log_file: "logs/etl1_scalable.log"
```

### Environment Variables

```bash
export ETL1_CHUNK_SIZE=100000
export ETL1_MEMORY_LIMIT_MB=16384
export ETL1_LOG_LEVEL=DEBUG
```

## Performance Tuning

### Chunk Size Optimization

| File Size | Recommended Chunk Size | Memory Usage |
|-----------|----------------------|--------------|
| < 1GB     | 25,000 - 50,000      | 2-4 GB       |
| 1-5GB     | 50,000 - 100,000     | 4-8 GB       |
| 5-20GB    | 100,000 - 200,000    | 8-16 GB      |
| > 20GB    | 200,000 - 500,000    | 16+ GB       |

### Memory Optimization

```python
# For large files, use smaller chunks
config = ETL1Config()
config.chunk_size = 25000  # Smaller chunks
config.memory_limit_mb = 4096  # Lower memory limit

# For high-memory systems
config.chunk_size = 200000  # Larger chunks
config.memory_limit_mb = 32768  # Higher memory limit
```

## Testing

### Run Test Suite

```bash
# Run all tests
python ETL/test_etl1_scalable.py

# Run specific test
python -c "
from ETL.test_etl1_scalable import test_small_chunks
test_small_chunks()
"
```

### Performance Benchmark

```bash
# Run performance benchmark
python ETL/test_etl1_scalable.py --benchmark
```

## Monitoring and Logging

### Log Files

- **Pipeline Logs**: `logs/etl1_scalable.log`
- **Error Logs**: `logs/etl1_scalable.error.log`
- **Performance Logs**: `logs/etl1_scalable.perf.log`

### Log Levels

- **DEBUG**: Detailed processing information
- **INFO**: General progress updates (default)
- **WARNING**: Warnings and non-critical issues
- **ERROR**: Errors that don't stop processing
- **CRITICAL**: Fatal errors that stop processing

### Progress Tracking

```
Processing rates chunk 1/20 (rows 0-50,000)
Processing rates chunk 2/20 (rows 50,000-100,000)
...
✅ ETL1 pipeline completed in 45.2 seconds
Processed 1,000,000 rates and 50,000 provider records
```

## Troubleshooting

### Common Issues

#### Memory Issues
```bash
# Reduce chunk size
python ETL/scripts/run_etl1_scalable.py --payer aetna --chunk-size 10000

# Increase memory limit
python ETL/scripts/run_etl1_scalable.py --payer aetna --memory-limit 16384
```

#### File Not Found
```bash
# Check input files
ls data/input/*.parquet

# Verify payer name
python ETL/scripts/run_etl1_scalable.py --payer aetna --dry-run
```

#### Performance Issues
```bash
# Enable verbose logging
python ETL/scripts/run_etl1_scalable.py --payer aetna --verbose

# Run performance benchmark
python ETL/test_etl1_scalable.py --benchmark
```

### Debug Mode

```bash
# Enable debug logging
python ETL/scripts/run_etl1_scalable.py --payer aetna --verbose

# Check configuration
python ETL/scripts/run_etl1_scalable.py --payer aetna --dry-run
```

## Architecture

### Processing Flow

1. **Configuration**: Load settings from YAML or command line
2. **Input Validation**: Verify input files exist and are readable
3. **Chunked Processing**: Process data in configurable chunks
4. **Dimension Processing**: Extract and upsert dimension tables
5. **Fact Processing**: Extract and upsert fact table
6. **Memory Cleanup**: Explicit cleanup between chunks
7. **Summary**: Generate processing summary

### Memory Management

- **Lazy Loading**: Only load required data
- **Chunked Processing**: Process small batches
- **DuckDB Upserts**: Efficient deduplication
- **Garbage Collection**: Explicit cleanup
- **Memory Monitoring**: Track memory usage

### Error Handling

- **File Validation**: Check input files exist
- **Chunk Recovery**: Continue processing after errors
- **Memory Limits**: Prevent out-of-memory errors
- **Logging**: Comprehensive error logging
- **Graceful Degradation**: Continue processing when possible

## Migration from Notebook

### Key Differences

| Notebook Version | Scalable Version |
|------------------|------------------|
| Loads entire files | Processes in chunks |
| In-memory joins | DuckDB upserts |
| No progress tracking | Real-time progress |
| Manual execution | Automated pipeline |
| No error handling | Comprehensive error handling |

### Migration Steps

1. **Install Dependencies**: Ensure all required packages are installed
2. **Prepare Data**: Ensure input files are in the correct location
3. **Configure Settings**: Set up configuration file or command line args
4. **Run Pipeline**: Execute the scalable version
5. **Validate Results**: Compare outputs with notebook version

## API Reference

### ETL1Config

```python
@dataclass
class ETL1Config:
    data_root: Path = Path("data")
    chunk_size: int = 50000
    memory_limit_mb: int = 8192
    state: str = "GA"
    payer_slug_override: Optional[str] = None
    # ... other fields
```

### ScalableETL1

```python
class ScalableETL1:
    def __init__(self, config: ETL1Config)
    def run_pipeline(self, payer: str) -> Dict[str, Any]
    def process_rates_chunk(self, chunk: pl.DataFrame) -> pl.DataFrame
    def process_providers_chunk(self, chunk: pl.DataFrame) -> pl.DataFrame
    def memory_efficient_upsert(self, df_new: pl.DataFrame, output_path: Path, keys: List[str], table_name: str)
```

## Contributing

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python ETL/test_etl1_scalable.py

# Run linting
flake8 ETL/etl1_scalable.py
```

### Adding New Features

1. **Update Configuration**: Add new config options to `ETL1Config`
2. **Implement Logic**: Add processing logic to `ScalableETL1`
3. **Add Tests**: Create tests in `test_etl1_scalable.py`
4. **Update Documentation**: Update this README
5. **Test Performance**: Run performance benchmarks

## License

This project is part of the workcomp-rates-etl system. See the main project license for details.
