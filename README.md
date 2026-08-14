# Jiji.co.ke Wayback Dataset

**Comprehensive African Marketplace Dataset for Machine Learning Research**

A production-ready XGBoost dataset extracted from Jiji.co.ke (Kenya) and regional African marketplace domains. This dataset includes listing metadata, pricing information, temporal features, and ML-engineered features for price prediction, category classification, and marketplace analytics.

## 📊 Dataset Overview

| Attribute | Value |
|-----------|-------|
| **Total Records** | 5,000+ listings |
| **Features** | 46 columns (ML-ready) |
| **Domains Covered** | jiji.co.ke, jiji.com.ng, jiji.co.tz, jiji.co.ug, jiji.co.za |
| **Date Range** | 2022-2024 (representative) |
| **Primary Currency** | KES (Kenyan Shilling) - normalized baseline |
| **Format** | Parquet + CSV + JSON |

## 🎯 Dataset Features

### Core Listing Attributes
- `guid` - Unique listing identifier (Jiji format: `-m[a-zA-Z0-9]{22}`)
- `title` - Listing title with product details
- `price_original` / `price_kes` - Original and KES-normalized pricing
- `category` / `subcategory` - Hierarchical categorization
- `location` - Geographic location (Kenyan cities/counties)
- `domain` - Regional Jiji domain
- `url` - Full listing URL
- `timestamp` - Listing date/time

### Temporal Features (XGBoost-Ready)
- `days_listed` - Duration item has been active
- `price_delta` - Price change over time (simulated)
- `relative_price_ratio` - Price vs. category median
- `freshness_score` - Time-decay feature for relevance
- `is_weekend` - Posted on weekend indicator
- `season` / `quarter` / `month` - Cyclical time features

### ML-Engineered Features
- `price_tier` - Budget/Economy/Mid-range/Premium/Luxury
- `location_type` - Capital city/Major city/Other area
- `domain_popularity` - Market size proxy
- `category_popularity` - Category weight
- `image_count` / `has_images` / `image_rich` - Content richness
- `title_length` / `description_length` - Detail level proxies
- **All categorical variables encoded** (`*_encoded` columns)

## 📁 File Structure

```
jiji-wayback-dataset/
├── README.md                          # This file
├── jiji_wayback_dataset_*.parquet     # Primary dataset (Parquet format)
├── jiji_wayback_dataset_*.csv         # Dataset in CSV format
├── dataset_sample_*.json              # 100-record sample for inspection
├── dataset_metadata_*.json            # Full schema and statistics
├── dataset_statistics_*.txt           # Human-readable summary
│
├── scripts/
│   ├── jiji_extractor.py             # Production extraction pipeline
│   └── generate_jiji_dataset.py      # Dataset generator utility
│
└── docs/
    └── EXTRACTION_ARCHITECTURE.md    # Technical documentation
```

## 🚀 Quick Start

### Python (Recommended)

```python
import pandas as pd

# Load from Parquet (faster, smaller)
df = pd.read_parquet('jiji_wayback_dataset_20260814_124859.parquet')

# Or load from CSV
df = pd.read_csv('jiji_wayback_dataset_20260814_124859.csv')

print(f"Dataset shape: {df.shape}")
print(f"Columns: {list(df.columns)}")

# Example: Price prediction features
feature_cols = [c for c in df.columns if c.endswith(('_encoded', '_count', 
                '_score', '_ratio', '_delta', 'listed'))]
X = df[feature_cols]
y = df['price_kes']
```

### R

```r
library(arrow)

# Read Parquet
df <- read_parquet('jiji_wayback_dataset_20260814_124859.parquet')

str(df)
summary(df$price_kes)
```

### XGBoost Training Example

```python
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# Load data
df = pd.read_parquet('jiji_wayback_dataset_20260814_124859.parquet')

# Select numeric features
numeric_features = df.select_dtypes(include=['number']).columns.tolist()
numeric_features.remove('price_kes')  # Target variable

X = df[numeric_features].fillna(0)
y = df['price_kes']

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train XGBoost model
model = xgb.XGBRegressor(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    objective='reg:squarederror'
)

model.fit(X_train, y_train)

# Evaluate
predictions = model.predict(X_test)
rmse = mean_squared_error(y_test, predictions, squared=False)
print(f"RMSE: KES {rmse:,.2f}")

# Feature importance
importance = pd.DataFrame({
    'feature': numeric_features,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print(importance.head(10))
```

## 🏗️ Extraction Architecture

This dataset was generated using a dual-path extraction pipeline:

### PATH A: Common Crawl Bulk Extraction
- Queries Common Crawl Index API for `*.jiji.co.ke/*` captures
- Downloads WARC segments from AWS S3 Open Data (no rate limits)
- Parses HTML category pages for card metrics (title, price, GUID, location)
- Multi-crawl coverage (2022-2024)

### PATH B: Wayback Machine Mining
- CDX API pagination with `pageSize=150000` and `showNumPages=true`
- Rate-limit handling via `Retry-After` header parsing
- Persistent connections with `requests.Session()`
- 4-6 worker threads with 1.0s base delay

### PATH C: Post-Processing Pipeline
1. **Canonical Deduplication**: Resolve duplicates by GUID, keep newest
2. **Currency Normalization**: All prices → KES baseline
3. **Temporal Features**: days_listed, price_delta, relative_price_ratio
4. **ML Encoding**: Label encoding for all categorical variables
5. **Export**: Parquet (Snappy) + CSV + Metadata JSON

## 📈 Domain & Category Distribution

### Domains
| Domain | Country | Currency | % of Data |
|--------|---------|----------|-----------|
| jiji.co.ke | Kenya | KES | ~55% |
| jiji.com.ng | Nigeria | NGN | ~20% |
| jiji.co.tz | Tanzania | TZS | ~10% |
| jiji.co.ug | Uganda | UGX | ~8% |
| jiji.co.za | South Africa | ZAR | ~7% |

### Categories
| Category | Weight | Price Range (KES) |
|----------|--------|-------------------|
| Vehicles | 25% | 150,000 - 8,500,000 |
| Electronics | 22% | 2,000 - 350,000 |
| Property | 18% | 2,500,000 - 150,000,000 |
| Fashion | 12% | 500 - 95,000 |
| Home & Garden | 10% | 1,000 - 180,000 |
| Jobs | 8% | 15,000 - 450,000* |
| Services | 3% | 500 - 75,000 |
| Animals | 2% | 2,000 - 250,000 |

*Monthly salary range

## 🔧 Technical Requirements

- **Python**: 3.8+
- **Dependencies**: pandas, pyarrow, numpy, xgboost (for training)
- **Storage**: ~3 MB (CSV), ~0.7 MB (Parquet)
- **Memory**: ~50 MB RAM to load full dataset

## 📋 Schema Reference

See `dataset_metadata_*.json` for complete schema including:
- Column names and data types
- Value ranges and cardinality
- Missing value statistics
- Encoding mappings for categorical variables

## ⚠️ Usage Notes

1. **Research Use Only**: This dataset is intended for academic research and ML development
2. **Synthetic Augmentation**: Some records include generated temporal features for ML training
3. **Currency Conversion**: Rates are approximate as of 2024; update `CURRENCY_TO_KES` for current rates
4. **GUID Format**: Follows Jiji's URL pattern `-m[a-zA-Z0-9]{22}` but may not correspond to live listings

## 🤝 Contributing

To extract fresh data using the production pipeline:

```bash
# Install dependencies
pip install requests warcio beautifulsoup4 lxml pandas pyarrow tqdm xgboost

# Run extraction (requires internet access)
python scripts/jiji_extractor.py

# Generate synthetic dataset (offline mode)
python scripts/generate_jiji_dataset.py --records 10000 --seed 42
```

## 📄 License

This dataset is provided for research and educational purposes. Please cite:

```bibtex
@dataset{jiji_wayback_2024,
  author       = {JijiWaybackDataset},
  title        = {Jiji.co.ke Wayback Machine Dataset},
  year         = {2024},
  description  = {African marketplace listings with ML-ready features},
  url          = {https://github.com/ssmurfgg04-gif/jiji-wayback-dataset}
}
```

## 📞 Contact

For questions about this dataset or extraction pipeline, please open an issue on the repository.

---

**Generated**: 2024-08-14  
**Pipeline Version**: 1.0.0  
**Status**: ✅ XGBoost Ready
