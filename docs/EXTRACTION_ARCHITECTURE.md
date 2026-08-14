# Jiji.co.ke Extraction Architecture

## Technical Documentation for Data Pipeline

This document describes the complete architecture used to extract, process, and prepare the Jiji.co.ke Wayback Dataset for machine learning applications.

---

## 1. System Overview

### 1.1 Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    JIJI EXTRACTION PIPELINE                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │   PATH A:        │    │   PATH B:        │    │  PATH C:     │  │
│  │   Common Crawl   │───▶│   Wayback Machine│───▶│ Post-Process │  │
│  │   Bulk Extract   │    │   API Mining     │    │ & Export     │  │
│  └──────────────────┘    └──────────────────┘    └──────────────┘  │
│         │                       │                      │           │
│         ▼                       ▼                      ▼           │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐     │
│  │ WARC Records │      │ CDX JSON     │      │ Deduplication│     │
│  │ HTML Cards   │      │ Item Metadata│      │ Currency Norm│     │
│  │ Category Pg  │      │ Timestamps   │      │ Temporal Feat│     │
│  └──────────────┘      └──────────────┘      └──────────────┘     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────┐
                    │   OUTPUT DATASETS          │
                    ├───────────────────────────┤
                    │ • jiji_dataset.parquet     │
                    │ • jiji_dataset.csv         │
                    │ • metadata.json            │
                    │ • statistics.txt           │
                    └───────────────────────────┘
```

### 1.2 Target Domains

| Domain | Country | Primary Use | Priority |
|--------|---------|-------------|----------|
| `jiji.co.ke` | Kenya | **Primary target** | P0 |
| `jiji.com.ng` | Nigeria | Regional expansion | P1 |
| `jiji.co.tz` | Tanzania | Regional expansion | P2 |
| `jiji.co.ug` | Uganda | Regional expansion | P2 |
| `jiji.co.za` | South Africa | Regional expansion | P2 |
| `jiji.com.et` | Ethiopia | Future coverage | P3 |
| `jiji.com.gh` | Ghana | Future coverage | P3 |

---

## 2. PATH A: Common Crawl Bulk Extraction

### 2.1 Architecture

```
Common Crawl Index API (index.commoncrawl.org)
        │
        ▼
Query: CC-MAIN-<ID>-index?url=*.jiji.co.ke/*&output=json
        │
        ▼
WARC Record Metadata (filename, offset, length)
        │
        ▼
AWS S3 Open Data (data.commoncrawl.org)
Range Request: bytes={offset}-{offset+length}
        │
        ▼
Raw WARC Response → HTTP Payload Extraction
        │
        ▼
HTML Content → BeautifulSoup Parser
        │
        ▼
Listing Card Extraction (GUID, Price, Title, Location)
```

### 2.2 API Endpoints

**Index Query**
```http
GET https://index.commoncrawl.org/CC-MAIN-<CRAWL_ID>-index?url=*.jiji.co.ke/*&output=json&limit=5000&filter=statuscode:200&filter=mimetype:text/html
```

**WARC Download**
```http
GET https://data.commoncrawl.org/<FILENAME>
Range: bytes=<OFFSET>-<OFFSET>+<LENGTH>
User-Agent: JijiDatasetResearchBot/1.0
```

### 2.3 Key Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `url` | `*.jiji.co.ke/*` | Wildcard for all subpages |
| `output` | `json` | JSON response format |
| `limit` | 5000 | Max records per crawl |
| `filter` | `statuscode:200` | Only successful responses |
| `filter` | `mimetype:text/html` | HTML pages only |

### 2.4 WARC Processing

```python
from warcio import ArchiveIterator
from io import BytesIO

def parse_warc_response(raw_data: bytes) -> str:
    """Extract HTTP payload from WARC record"""
    stream = BytesIO(raw_data)
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            return record.content_stream().read().decode('utf-8')
    return None
```

### 2.5 HTML Card Parsing Strategy

Jiji.co.ke uses specific HTML patterns for listing cards:

```html
<!-- Pattern 1: data-qaid attribute -->
<div class="b-advert" data-qaid="advert-card">
  <h3 class="qa-advert-title">Toyota Corolla 2020</h3>
  <div class="qa-advert-price">KSh 1,250,000</div>
  <div class="qa-advert-location">Nairobi, Karen</div>
</div>

<!-- Pattern 2: GUID in URL -->
<a href="/cars/toyota-corolla-mBVuBBJi2x0v8wazh0DuOSEY">
  <span>Toyota Corolla - Like New</span>
</a>
```

**Regex Patterns**
```python
# GUID extraction from URL
GUID_PATTERN = re.compile(r'-m([a-zA-Z0-9]{22})')

# Price extraction (KES format)
PRICE_PATTERN = re.compile(r'(?:KSh|KES)\s*([\d,]+)')

# Category page detection
CATEGORY_URLS = ['/cars', '/phones', '/electronics', '/fashion', 
                 '/property', '/jobs', '/services', '/animals']
```

---

## 3. PATH B: Wayback Machine Mining

### 3.1 Connection Architecture

```python
import requests
from concurrent.futures import ThreadPoolExecutor

class WaybackMiner:
    def __init__(self):
        # Persistent session (connection pooling)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'JijiDatasetResearchBot/1.0 (academic research)'
        })
        
        # Concurrency settings
        self.max_workers = 4  # 4-6 threads max
        self.base_delay = 1.0  # 1 second between requests
```

### 3.2 CDX Pagination API

**Endpoint**: `https://web.archive.org/cdx/search/cdx`

**Pagination Parameters**
```python
params = {
    'url': '*.jiji.co.ke/*',
    'output': 'json',
    'from': '20220101',
    'to': '20241215',
    'pageSize': 150000,       # Maximum efficient size
    'page': 0,                # Current page
    'showNumPages': 'true',   # Get total page count
    'filter': 'statuscode:200',
    'collapse': 'urlkey'      # Deduplicate by URL
}
```

**Response Format**
```json
[
  ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
  ["20240115123456", "https://jiji.co.ke/cars/toyota-abc123", "text/html", "200", "SHA256...", "12345"],
  ...
]
```

### 3.3 Rate-Limit Handling

```python
def _make_request(self, url, params, max_retries=5):
    for attempt in range(max_retries):
        resp = self.session.get(url, params=params, timeout=120)
        
        if resp.status_code == 429:
            # Parse Retry-After header dynamically
            retry_after = resp.headers.get('Retry-After')
            
            if retry_after:
                wait_time = float(retry_after)
            else:
                # Exponential backoff fallback
                wait_time = 2 ** attempt
            
            logger.warning(f"Rate limited. Waiting {wait_time}s...")
            time.sleep(wait_time)
            continue
        
        elif resp.status_code == 200:
            return resp
    
    return None  # All retries exhausted
```

### 3.4 Worker Thread Pool

```python
with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
    futures = {}
    
    for page_num in range(total_pages):
        future = executor.submit(
            self._fetch_cdx_page,
            domain,
            page_num
        )
        futures[future] = page_num
        
        # Rate limiting between submissions
        time.sleep(self.base_delay)
    
    # Collect results
    for future in as_completed(futures, timeout=300):
        records = future.result()
        all_records.extend(records)
```

---

## 4. PATH C: Post-Processing Pipeline

### 4.1 Processing Flow

```
Raw Listings (CC + Wayback)
        │
        ▼
┌───────────────────┐
│ Step 1: Dedup     │ ← Resolve by GUID, keep newest
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Step 2: Currency  │ ← Normalize to KES baseline
│     Normalization │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Step 3: Temporal  │ ← days_listed, price_delta,
│     Features      │   relative_price_ratio
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Step 4: ML Encode │ ← Label encoding for categoricals
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ Step 5: Export    │ ← Parquet + CSV + Metadata
└───────────────────┘
```

### 4.2 Canonical Deduplication

```python
def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve duplicate captures using unique item IDs/GUIDs.
    
    Strategy:
    1. Sort by timestamp descending (newest first)
    2. Drop exact duplicates (same guid + timestamp)
    3. For each GUID, keep most recent as canonical record
    4. Preserve full history for temporal feature calculation
    """
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp', ascending=False)
    
    # Remove exact duplicates
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['guid', 'timestamp'], keep='first')
    
    # Canonical records (newest per GUID)
    canonical_df = df.drop_duplicates(subset=['guid'], keep='first')
    
    return canonical_df
```

### 4.3 Currency Normalization

**Conversion Rates (to KES baseline)**

| Currency | Symbol | Rate to KES | Formula |
|----------|--------|-------------|---------|
| KES | KSh | 1.0 | `price * 1.0` |
| NGN | ₦ | 0.085 | `price * 0.085` |
| TZS | TSh | 0.053 | `price * 0.053` |
| UGX | USh | 0.078 | `price * 0.078` |
| ZAR | R | 7.8 | `price * 7.8` |

```python
CURRENCY_TO_KES = {
    'KES': 1.0,
    'NGN': 0.085,
    'TZS': 0.053,
    'UGX': 0.078,
    'ZAR': 7.8,
}

def normalize_currency(row):
    currency = row.get('currency', 'KES')
    rate = CURRENCY_TO_KES.get(currency, 1.0)
    return row['price_original'] * rate
```

### 4.4 Temporal Feature Engineering

#### 4.4.1 Days Listed
```python
# Time between first and last appearance of a GUID
temp_features['days_listed'] = (
    temp_features['last_seen'] - temp_features['first_seen']
).dt.days
```

#### 4.4.2 Price Delta
```python
# Price change over listing lifetime (indicates negotiation/drops)
temp_features['price_delta'] = (
    temp_features['latest_price'] - temp_features['initial_price']
)
```

#### 4.4.3 Relative Price Ratio
```python
# Item price vs category median (market positioning)
category_medians = temp_features.groupby('category')['latest_price'].transform('median')

temp_features['relative_price_ratio'] = (
    temp_features['latest_price'] / category_medians
).fillna(1.0).replace([np.inf, -np.inf], 1.0)
```

#### 4.4.4 Additional Time Features
```python
df['is_weekend'] = df['timestamp'].dt.dayofweek.isin([5, 6]).astype(int)
df['hour_of_day'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.dayofweek
df['month'] = df['timestamp'].dt.month
df['quarter'] = df['timestamp'].dt.quarter
df['season'] = df['month'].apply(get_season)

# Freshness score (exponential decay)
max_timestamp = df['timestamp'].max()
df['days_since_posting'] = (max_timestamp - df['timestamp']).dt.days
df['freshness_score'] = 1 / (1 + df['days_since_posting'] / 30)
```

### 4.5 Categorical Encoding

```python
categorical_cols = [
    'category', 'subcategory', 'domain', 'currency',
    'crawl_source', 'price_tier', 'location_type', 'season'
]

for col in categorical_cols:
    if col in df.columns:
        # Label encoding (0, 1, 2, ...)
        df[f'{col}_encoded'] = pd.Categorical(df[col]).codes
```

---

## 5. Output Schema

### 5.1 Final Column List (46 features)

**Identifiers**
- `guid` - Unique listing ID (string)
- `listing_id` - Human-readable ID (string)
- `url` - Full URL (string)
- `title` - Listing title (string)

**Pricing (7 columns)**
- `price_original` - Original price (float)
- `currency` - Original currency code (string)
- `price_kes` - KES-normalized price (float)
- `price_tier` - Budget/Economy/Mid/Premium/Luxury (string)
- `price_tier_encoded` - Encoded tier (int)
- `price_delta` - Price change over time (float)
- `relative_price_ratio` - vs category median (float)

**Category & Location (7 columns)**
- `category` - Main category (string)
- `category_encoded` - Encoded category (int)
- `category_popularity` - Weight proxy (float)
- `subcategory` - Sub-category (string)
- `subcategory_encoded` - Encoded subcategory (int)
- `location` - Geographic location (string)
- `location_type` - City type classification (string)
- `location_type_encoded` - Encoded location type (int)

**Domain (3 columns)**
- `domain` - Regional domain (string)
- `domain_encoded` - Encoded domain (int)
- `domain_popularity` - Market size proxy (float)

**Content Features (9 columns)**
- `description` - Item description (string)
- `seller_name` - Seller identifier (string)
- `image_count` - Number of images (int)
- `has_images` - Binary flag (int)
- `image_rich` - 4+ images flag (int)
- `title_length` - Character count (int)
- `title_word_count` - Word count (int)
- `has_description` - Binary flag (int)
- `description_length` - Description char count (int)

**Temporal Features (12 columns)**
- `timestamp` - Listing datetime (datetime)
- `crawl_source` - Data source (string)
- `crawl_source_encoded` - Encoded source (int)
- `first_seen` - First appearance (datetime)
- `days_listed` - Active duration (int)
- `days_since_posting` - Age in days (int)
- `freshness_score` - Decay score (float)
- `is_weekend` - Weekend posting (int)
- `hour_of_day` - Posting hour (int)
- `day_of_week` - Day number (int)
- `month` - Month number (int)
- `quarter` - Quarter number (int)
- `season` - Season name (string)
- `season_encoded` - Encoded season (int)

### 5.2 Data Types

| Type | Columns | Count |
|------|---------|-------|
| `int64` | image_count, has_images, *_encoded, days_*, hour, day, month, quarter | ~20 |
| `float64` | price_*, ratio, delta, popularity, score, length | ~12 |
| `object/string` | guid, title, url, category, location, description, etc. | ~14 |
| `datetime64` | timestamp, first_seen | 2 |

---

## 6. Performance Characteristics

### 6.1 Expected Volume Estimates

Based on Common Crawl index queries:

| Domain | Est. Pages/Crawl | Crawls Covered | Total Potential |
|--------|------------------|----------------|-----------------|
| jiji.co.ke | ~3,040 | 10 (2022-2024) | ~30,400 |
| jiji.com.ng | ~5,200 | 10 | ~52,000 |
| jiji.co.tz | ~800 | 10 | ~8,000 |
| jiji.co.ug | ~600 | 10 | ~6,000 |
| jiji.co.za | ~2,100 | 10 | ~21,000 |

**Note**: Actual extracted volume depends on filtering and deduplication.

### 6.2 Processing Time Estimates

| Operation | 1k Records | 10k Records | 100k Records |
|-----------|------------|-------------|--------------|
| CC Index Query | ~30s | ~60s | ~120s |
| WARC Download | ~2min | ~15min | ~90min |
| HTML Parsing | ~10s | ~90s | ~15min |
| CDX Query | ~45s | ~3min | ~20min |
| Post-Processing | ~5s | ~30s | ~4min |
| **Total** | **~3min** | **~20min** | **~2hr** |

### 6.3 Storage Requirements

| Format | Size/Record | 5k Records | 50k Records |
|--------|-------------|------------|-------------|
| CSV | ~500B | 2.5 MB | 25 MB |
| Parquet (Snappy) | ~150B | 750 KB | 7.5 MB |
| JSON | ~800B | 4 MB | 40 MB |

---

## 7. Error Handling & Resilience

### 7.1 Network Errors

```python
# Connection timeout handling
try:
    resp = session.get(url, timeout=120)
except requests.exceptions.ConnectTimeout:
    logger.warning(f"Connection timeout: {url}")
    return None
except requests.exceptions.ConnectionError:
    logger.warning(f"Connection error: {url}")
    return None
```

### 7.2 Rate Limiting (HTTP 429)

```python
if resp.status_code == 429:
    retry_after = resp.headers.get('Retry-After')
    
    if retry_header:
        wait_time = float(retry_header)
    else:
        wait_time = 2 ** attempt  # Exponential backoff
    
    time.sleep(wait_time)
```

### 7.3 Resume Capability

The pipeline supports resuming interrupted extractions:

```python
# Checkpoint file tracks progress
CHECKPOINT_FILE = '.extraction_progress.json'

def save_checkpoint(state):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(state, f)

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return None
```

---

## 8. Security Considerations

### 8.1 User-Agent Identification

All requests include proper identification:

```python
headers = {
    'User-Agent': 'JijiDatasetResearchBot/1.0 (academic research; mailto:research@example.com)',
    'Accept': 'application/json, text/html',
    'Accept-Encoding': 'gzip, deflate'
}
```

### 8.2 Rate Limiting Compliance

- Base delay: 1.0 second between requests
- Maximum workers: 4-6 threads
- Dynamic `Retry-After` header parsing
- Exponential backoff when headers missing

### 8.3 Data Privacy

- No personal seller information retained beyond public listings
- Email addresses and phone numbers stripped during parsing
- Only aggregate statistics reported

---

## 9. Future Enhancements

### Planned Features

1. **Real-time Monitoring**: Webhook notifications for new captures
2. **Distributed Processing**: Dask/Spark for >1M records
3. **Image Analysis**: Computer vision for product condition assessment
4. **NLP Features**: Text embeddings from titles/descriptions
5. **Geospatial**: Coordinate extraction from locations
6. **Time Series**: ARIMA/Prophet for price forecasting

### Known Limitations

1. **Network Dependency**: Requires internet access for live extraction
2. **Rate Limits**: Wayback may throttle aggressive crawling
3. **HTML Fragility**: Site structure changes break parsers
4. **Currency Volatility**: Conversion rates become stale

---

## Appendix A: Sample Record

```json
{
  "guid": "-mBVuBBJi2x0v8wazh0DuOSEY",
  "listing_id": "JIJI-A1B2C3D4E5F6",
  "url": "https://jiji.co.ke/vehicles-toyota-corolla-2020-like-new-mBVuBBJi2x0v8wazh0DuOSEY",
  "title": "Toyota Corolla 2020 - Like New - Nairobi Registered",
  "price_original": 1250000.0,
  "currency": "KES",
  "price_kes": 1250000.0,
  "price_tier": "mid_range",
  "price_tier_encoded": 2,
  "price_delta": -50000.0,
  "relative_price_ratio": 0.95,
  "category": "Vehicles",
  "category_encoded": 0,
  "category_popularity": 25.0,
  "subcategory": "Cars",
  "subcategory_encoded": 0,
  "location": "Nairobi, Karen",
  "location_type": "capital_city",
  "location_type_encoded": 0,
  "domain": "jiji.co.ke",
  "domain_encoded": 0,
  "domain_popularity": 1.0,
  "description": "Genuine vehicle in excellent condition...",
  "seller_name": "Nairobi Motors Dealer",
  "image_count": 8,
  "has_images": 1,
  "image_rich": 1,
  "title_length": 48,
  "title_word_count": 8,
  "has_description": 1,
  "description_length": 95,
  "timestamp": "2024-01-15T14:32:00",
  "crawl_source": "common_crawl",
  "crawl_source_encoded": 0,
  "first_seen": "2024-01-15T14:32:00",
  "days_listed": 0,
  "days_since_posting": 334,
  "freshness_score": 0.082,
  "is_weekend": 0,
  "hour_of_day": 14,
  "day_of_week": 0,
  "month": 1,
  "quarter": 1,
  "season": "Summer",
  "season_encoded": 0
}
```

---

**Document Version**: 1.0.0  
**Last Updated**: 2024-08-14  
**Author**: JijiWaybackDataset Team
