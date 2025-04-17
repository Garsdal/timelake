# ⏳ timelake

**A time series feature framework**  
Built for production ML. Fast. Safe. Flexible.

## What is Timelake?

Timelake is a framework and SDK for working with time series data and building time-based features for machine learning. Think of it as a lakehouse tailored for time — combining fast, expressive feature engineering with robust data management.

- 📦 Powered by [Delta Lake](https://delta.io) for versioned storage  
- ⚙️ Core engine in **Rust** for speed, with **Python bindings**  
- 🧮 Feature definitions using **Polars** under the hood  
- 🧠 Designed to prevent data leakage, one `horizon` at a time  

## Key Features

### Storage Support
Timelake now supports multiple storage backends, making it flexible for both local and cloud environments:
- **LOCAL**: Store your data locally on disk.
- **S3**: Seamlessly integrate with Amazon S3 using environment variables for credentials.

#### Example: Local Storage
```python
from timelake import TimeLake
from timelake.constants import StorageType

lake = TimeLake.create(
    path="./my-local-timelake",
    df=df,
    timestamp_column="date",
    storage_type=StorageType.LOCAL,
)
```

#### Example: S3 Storage
```python
import dotenv
from timelake import TimeLake
from timelake.constants import StorageType

dotenv.load_dotenv()  # Load AWS credentials from environment variables

lake = TimeLake.create(
    path="s3://my-time-lake",
    df=df,
    timestamp_column="date",
    storage_type=StorageType.S3,
)
```

### `TimeLake`
The central object for managing your time series datasets — think of it as your feature store for temporal data.

#### Create
Create a new TimeLake instance and write data to it:
```python
lake = TimeLake.create(
    path="./my-local-timelake",
    df=df,
    timestamp_column="date",
    storage_type=StorageType.LOCAL,
)
```

#### Read
Read data from the TimeLake:
```python
df = lake.read(start_date="2024-01-01", end_date="2024-01-03")
```

#### Write
Write new data to the TimeLake:
```python
lake.write(df=new_data, mode="append")
```

#### Upsert
Upsert data into the TimeLake:
```python
lake.upsert(df_update)
```

### `TimeFeature`
Features live here. Define, transform, and retrieve time-based features safely.

```python
from timelake import TimeFeature

# Create a lag feature with a 7-day horizon to avoid leakage
f = TimeFeature("consumption").lag(days=1, horizon="7d")
```

## Why Timelake?
- 🔒 Leakage-aware by design
- 🔄 Simple and expressive feature transformations
- 🚀 Blazing fast via Polars and Rust
- 📁 Delta-native for easy integration with data lakes
- 🌐 Flexible storage options (LOCAL and S3)

## Installation
Coming soon: `pip install timelake`

(For now, clone the repo and install locally.)

## Roadmap
- Add support for GCS and Azure Blob Storage
- Expand feature engineering capabilities
- Improve documentation and examples
- Add advanced `TimeFeature` transformations and support for custom feature definitions

## Contributing
TBD