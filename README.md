# ⏳ timelake

**A time series feature framework**  
Built for production ML. Fast. Safe. Flexible.

## What is Timelake?

Timelake is a framework and SDK for working with time series data and building time-based features for machine learning. Think of it as a lakehouse tailored for time — combining fast, expressive feature engineering with robust data management.

- 📦 Powered by [Delta Lake](https://delta.io) for versioned storage  
- ⚙️ Core engine in **Rust** for speed, with **Python bindings**  
- 🧮 Feature definitions using **Polars** under the hood  
- 🧠 Designed to prevent data leakage, one `horizon` at a time  

## Key Concepts

### `TimeLake`

The central object for managing your time series datasets — think of it as your feature store for temporal data.

```python
from timelake import TimeLake

lake = TimeLake("path/to/delta-table")
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

## Installation
Coming soon: `pip install timelake`

(For now, clone the repo and install locally.)

## Roadmap
TBD

## Contributing
TBD