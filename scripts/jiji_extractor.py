#!/usr/bin/env python3
"""
Jiji.co.ke Comprehensive Data Extraction Pipeline
=================================================
Dual-path extraction: Common Crawl (bulk HTML) + Wayback Machine (full JSON)
Output: XGBoost-ready Parquet/CSV dataset with temporal features

Architecture:
  PATH A: Common Crawl Index → WARC Range Fetch → HTML Card Parse
  PATH B: Wayback CDX Pagination → Item JSON Extraction  
  PATH C: Deduplication → Currency Normalization → Temporal Features → Export

Author: Automated Pipeline
Date: 2026-08-14
"""

import os
import sys
import json
import re
import time
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/home/z/my-project/download/jiji-dataset/extraction.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
OUTPUT_DIR = Path('/home/z/my-project/download/jiji-dataset')
CC_INDEX_BASE = 'https://index.commoncrawl.org/'
CC_DATA_BASE = 'https://data.commoncrawl.org/'
WAYBACK_CDX = 'https://web.archive.org/cdx/search/cdx'
WAYBACK_API = 'https://web.archive.org/web/id/'

# Target domains for regional coverage
TARGET_DOMAINS = [
    'jiji.co.ke',   # Kenya (primary)
    'jiji.com.ng',  # Nigeria
    'jiji.co.tz',   # Tanzania
    'jiji.co.ug',   # Uganda
    'jiji.co.za',   # South Africa
    'jiji.com.et',  # Ethiopia
    'jiji.com.gh',  # Ghana
]

# Currency mapping to KES (approximate rates for normalization)
CURRENCY_TO_KES = {
    'KES': 1.0,
    'KSh': 1.0,
    'NGN': 0.085,   # Nigerian Naira
    'TZS': 0.053,   # Tanzanian Shilling
    'UGX': 0.078,   # Ugandan Shilling
    'ZAR': 7.8,     # South African Rand
    'ETB': 0.72,    # Ethiopian Birr
    'GHS': 11.2,    # Ghanaian Cedi
    'USD': 129.5,   # US Dollar (approximate)
}


@dataclass
class ListingRecord:
    """Normalized listing record from any source"""
    guid: str
    title: str
    price_original: float
    currency: str
    price_kes: float
    location: str
    category: str
    subcategory: str
    domain: str
    url: str
    timestamp: str
    crawl_source: str  # 'common_crawl' or 'wayback'
    listing_id: Optional[str] = None
    description: Optional[str] = None
    seller_name: Optional[str] = None
    image_count: int = 0
    days_listed: Optional[int] = None
    price_delta: Optional[float] = None
    relative_price_ratio: Optional[float] = None


class CommonCrawlMiner:
    """
    PATH A: Zero Rate-Limit Common Crawl Bulk Extraction
    
    Queries CC Index API, fetches WARC segments from S3,
    parses HTML category pages for card metrics.
    """
    
    def __init__(self, max_crawls: int = 10):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'JijiDatasetResearchBot/1.0 (academic research)'
        })
        self.max_crawls = max_crawls
        self.crawl_ids = []
        
    def get_recent_crawl_ids(self) -> List[str]:
        """Get recent Common Crawl IDs for querying"""
        logger.info("Fetching recent Common Crawl IDs...")
        
        try:
            resp = self.session.get('https://index.commoncrawl.org/collinfo.json', timeout=30)
            resp.raise_for_status()
            crawls = resp.json()
            self.crawl_ids = [c['id'] for c in crawls[:self.max_crawls]]
            logger.info(f"Found {len(self.crawl_ids)} recent crawls: {self.crawl_ids[:3]}...")
            return self.crawl_ids
        except Exception as e:
            logger.error(f"Failed to get crawl IDs: {e}")
            # Fallback to known recent crawls
            fallback = [
                'CC-MAIN-2024-50', 'CC-MAIN-2024-46', 'CC-MAIN-2024-42',
                'CC-MAIN-2024-38', 'CC-MAIN-2024-34', 'CC-MAIN-2024-30',
                'CC-MAIN-2024-26', 'CC-MAIN-2024-22', 'CC-MAIN-2024-18',
                'CC-MAIN-2024-14'
            ]
            self.crawl_ids = fallback[:self.max_crawls]
            return self.crawl_ids
    
    def query_domain_index(self, domain: str) -> List[Dict]:
        """Query CC Index API for a specific domain's captures"""
        results = []
        
        if not self.crawl_ids:
            self.get_recent_crawl_ids()
            
        for crawl_id in self.crawl_ids:
            try:
                url = f"{CC_INDEX_BASE}{crawl_id}-index"
                params = {
                    'url': f"*.{domain}/*",
                    'output': 'json',
                    'limit': 5000,
                    'filter': ['statuscode:200', 'mimetype:text/html']
                }
                
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                
                records = resp.json()
                if records:
                    for r in records:
                        r['_crawl_id'] = crawl_id
                    results.extend(records)
                    
                logger.info(f"  {crawl_id}: {len(records)} records for {domain}")
                time.sleep(0.1)  # Be polite
                
            except Exception as e:
                logger.warning(f"  {crawl_id} query failed: {e}")
                
        logger.info(f"Total CC records for {domain}: {len(results)}")
        return results
    
    def filter_category_pages(self, records: List[Dict]) -> List[Dict]:
        """Filter to category/listing pages (not item detail pages)"""
        category_pages = []
        
        for rec in records:
            url = rec.get('url', '')
            # Category pages have patterns like /cars, /phones, /electronics etc.
            # Item pages have GUIDs in URL like -mBVuBBJi2x0v8wazh0DuOSEY
            if not re.search(r'-m[a-zA-Z0-9]{15,}', url):
                # Check it's a main domain page (likely category)
                if any(pattern in url.lower() for pattern in [
                    '/cars', '/phones', '/electronics', '/fashion', 
                    '/property', '/jobs', '/services', '/animals',
                    '/mobile-', '/laptops', '/house', '/land'
                ]) or len(url.split('/')) <= 5:
                    category_pages.append(rec)
                    
        logger.info(f"Filtered to {len(category_pages)} category pages")
        return category_pages
    
    def fetch_warc_record(self, record: Dict) -> Optional[str]:
        """Fetch single WARC record content by range"""
        try:
            filename = record.get('filename', '')
            offset = int(record.get('offset', 0))
            length = int(record.get('length', 0))
            
            if not all([filename, offset, length]):
                return None
                
            url = f"{CC_DATA_BASE}{filename}"
            headers = {'Range': f'bytes={offset}-{offset + length - 1}'}
            
            resp = self.session.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            
            return resp.content
            
        except Exception as e:
            logger.debug(f"WARC fetch failed: {e}")
            return None
    
    def parse_warc_response(self, raw_data: bytes) -> Optional[str]:
        """Extract HTTP payload from WARC record"""
        try:
            from warcio import ArchiveIterator
            from io import BytesIO
            
            stream = BytesIO(raw_data)
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    content = record.content_stream().read()
                    return content.decode('utf-8', errors='ignore')
        except ImportError:
            # Fallback: manual WARC parsing
            try:
                # Find double newline separating headers from body
                idx = raw_data.find(b'\r\n\r\n')
                if idx != -1:
                    # Skip HTTP response headers
                    body = raw_data[idx+4:]
                    # Handle chunked transfer encoding
                    if body.startswith(b'\r\n'):
                        body = body[2:]
                    return body.decode('utf-8', errors='ignore')
            except:
                pass
        except Exception as e:
            logger.debug(f"WARC parse error: {e}")
        return None
    
    def extract_listings_from_html(self, html: str, url: str, timestamp: str, domain: str) -> List[ListingRecord]:
        """Parse HTML category page and extract listing cards"""
        listings = []
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Jiji.co.ke uses various card containers
            # Pattern 1: Main listing cards with data-qaid attribute
            cards = soup.find_all(['div', 'article'], class_=re.compile(
                r'listing|card|item|b-advert|qa-advert', re.I
            ))
            
            if not cards:
                # Pattern 2: Look for links with GUID pattern
                cards = soup.find_all('a', href=re.compile(r'-m[a-zA-Z0-9]{15,}'))
            
            for card in cards:
                try:
                    record = self._parse_card(card, url, timestamp, domain)
                    if record and record.price_original > 0:
                        listings.append(record)
                except Exception as e:
                    continue
                    
        except Exception as e:
            logger.debug(f"HTML parse error: {e}")
            
        return listings
    
    def _parse_card(self, card, page_url: str, timestamp: str, domain: str) -> Optional[ListingRecord]:
        """Extract data from a single listing card"""
        
        # Extract GUID from link
        link = card.find('a', href=True) if hasattr(card, 'find') else None
        if not link:
            link = card if hasattr(card, 'get') and card.get('href') else None
            
        href = link.get('href', '') if link else ''
        guid_match = re.search(r'-m([a-zA-Z0-9]{15,})', href)
        guid = guid_match.group(1) if guid_match else hashlib.md5(href.encode()).hexdigest()[:16]
        
        # Extract title
        title_elem = card.find(['h2', 'h3', 'h4', 'span', 'p'], class_=re.compile(r'title|name|heading', re.I))
        if not title_elem:
            title_elem = card.find(string=re.compile(r'.{10,}'))
        title = title_elem.get_text(strip=True)[:200] if title_elem else 'Unknown'
        
        # Extract price
        price_text = ''
        price_elem = card.find(class_=re.compile(r'price|cost|value', re.I))
        if price_elem:
            price_text = price_elem.get_text(strip=True)
        else:
            # Try to find price pattern in card text
            text = card.get_text()
            price_match = re.search(r'(KSh|KES|₦|\$|R\s?|ETB|GH₵)\s*[\d,.]+', text)
            if price_match:
                price_text = price_match.group()
        
        price, currency = self._parse_price(price_text)
        
        # Extract location
        location = ''
        loc_elem = card.find(class_=re.compile(r'location|city|area|region', re.I))
        if loc_elem:
            location = loc_elem.get_text(strip=True)[:100]
        
        # Determine category from URL path
        category = self._extract_category(page_url)
        
        # Calculate normalized price
        price_kes = price * CURRENCY_TO_KES.get(currency, 1.0)
        
        return ListingRecord(
            guid=guid,
            title=title,
            price_original=price,
            currency=currency,
            price_kes=price_kes,
            location=location,
            category=category,
            subcategory='',
            domain=domain,
            url=f"https://{domain}{href}" if href.startswith('/') else href,
            timestamp=timestamp,
            crawl_source='common_crawl',
            image_count=0
        )
    
    def _parse_price(self, price_str: str) -> Tuple[float, str]:
        """Parse price string into numeric value and currency"""
        if not price_str:
            return 0.0, 'KES'
            
        currency = 'KES'
        
        # Detect currency
        curr_match = re.match(r'(KSh|KES|₦|NGN|\$|USD|R\s?|ZAR|ETB|GH₵|GHS)\s*', price_str)
        if curr_match:
            curr_sym = curr_match.group(1)
            currency_map = {
                'KSh': 'KES', 'KES': 'KES', '₦': 'NGN', 'NGN': 'NGN',
                '$': 'USD', 'USD': 'USD', 'R': 'ZAR', 'ZAR': 'ZAR',
                'ETB': 'ETB', 'GH₵': 'GHS', 'GHS': 'GHS'
            }
            currency = currency_map.get(curr_sym, 'KES')
        
        # Extract numeric value
        num_match = re.search(r'[\d,.]+', price_str.replace(',', ''))
        if num_match:
            try:
                price = float(num_match.group().replace(',', ''))
                return price, currency
            except ValueError:
                pass
                
        return 0.0, currency
    
    def _extract_category(self, url: str) -> str:
        """Extract category from URL path"""
        path_parts = url.split('/')
        for part in path_parts:
            if part in ['cars', 'phones', 'electronics', 'fashion', 'property', 
                       'jobs', 'services', 'animals', 'mobile-phones', 'laptops',
                       'houses', 'land', 'health', 'beauty', 'home', 'garden']:
                return part.capitalize()
        return 'General'


class WaybackMiner:
    """
    PATH B: Optimized Wayback Machine Mining
    
    Uses CDX pagination API with rate-limit handling.
    Mines full item JSON records for complete data.
    """
    
    def __init__(self, max_workers: int = 4, delay: float = 1.0):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'JijiDatasetResearchBot/1.0 (academic research; mailto:research@example.com)'
        })
        self.max_workers = max_workers
        self.delay = delay
        self.rate_limit_hits = 0
        
    def query_cdx_api(self, domain: str, from_date: str = '2020', to_date: str = None) -> List[Dict]:
        """
        Query Wayback CDX API with proper pagination.
        Uses showNumPages + pageSize for efficient bulk extraction.
        """
        if not to_date:
            to_date = datetime.now().strftime('%Y%m%d')
            
        all_records = []
        page = 0
        page_size = 150000  # Maximum efficient page size
        
        logger.info(f"Querying Wayback CDX for {domain} ({from_date} to {to_date})")
        
        while True:
            params = {
                'url': f'*.{domain}/*',
                'output': 'json',
                'from': from_date,
                'to': to_date,
                'pageSize': page_size,
                'page': page,
                'showNumPages': 'true',
                'filter': 'statuscode:200',
                'collapse': 'urlkey'  # Deduplicate by URL
            }
            
            try:
                resp = self._make_request(WAYBACK_CDX, params)
                
                if not resp:
                    break
                    
                data = resp.json()
                
                # First element is field names
                if isinstance(data, list) and len(data) > 1:
                    fields = data[0]
                    records = data[1:]
                    
                    for rec in records:
                        record_dict = dict(zip(fields, rec))
                        record_dict['_domain'] = domain
                        all_records.append(record_dict)
                        
                    logger.info(f"  Page {page}: {len(records)} records (total: {len(all_records)})")
                    
                    # Check if we got fewer records than requested (last page)
                    if len(records) < page_size:
                        break
                        
                elif isinstance(data, dict) and 'pages' in data:
                    total_pages = data['pages']
                    logger.info(f"  Total pages available: {total_pages}")
                    
                page += 1
                
                # Safety limit
                if page > 100:
                    logger.warning("Reached page safety limit")
                    break
                    
                time.sleep(self.delay)
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error on page {page}: {e}")
                break
            except Exception as e:
                logger.error(f"CDX query error on page {page}: {e}")
                break
                
        logger.info(f"Total CDX records for {domain}: {len(all_records)}")
        return all_records
    
    def _make_request(self, url: str, params: Dict, max_retries: int = 5) -> Optional[requests.Response]:
        """Make request with rate-limit handling and retry logic"""
        
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=120)
                
                if resp.status_code == 429:
                    self.rate_limit_hits += 1
                    
                    # Parse Retry-After header
                    retry_after = resp.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            wait_time = 2 ** attempt  # Exponential backoff
                    else:
                        wait_time = 2 ** attempt
                        
                    logger.warning(f"Rate limited (hit #{self.rate_limit_hits}). Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
                elif resp.status_code == 200:
                    return resp
                    
                elif resp.status_code == 503:
                    logger.warning(f"Service unavailable, retrying... (attempt {attempt+1})")
                    time.sleep(2 ** attempt)
                    continue
                    
                else:
                    logger.warning(f"HTTP {resp.status_code}, retrying...")
                    time.sleep(self.delay)
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                time.sleep(2 ** attempt)
                
        return None
    
    def fetch_item_json(self, timestamp: str, url: str) -> Optional[Dict]:
        """Fetch full item JSON from Wayback Memento API"""
        try:
            wayback_url = f"{timestamp}/{url}"
            params = {'format': 'json'}
            
            resp = self._make_request(f"{WAYBACK_API}{wayback_url.split('/')[-2]}", params)
            
            if resp and resp.status_code == 200:
                # Try to extract JSON from the archived page
                data = resp.json()
                return data
                
        except Exception as e:
            logger.debug(f"Item JSON fetch failed: {e}")
            
        return None


class DataProcessor:
    """
    PATH C: Post-Processing & XGBoost Feature Pipeline
    
    Handles deduplication, currency normalization,
    temporal feature extraction, and final export.
    """
    
    def __init__(self):
        self.listings: List[ListingRecord] = []
        self.df: Optional[pd.DataFrame] = None
        
    def add_listings(self, listings: List[ListingRecord]):
        """Add listings from any source"""
        self.listings.extend(listings)
        
    def deduplicate(self) -> pd.DataFrame:
        """
        Canonical Deduplication:
        - Resolve duplicate captures using unique item IDs/GUIDs
        - Keep most recent version as primary
        """
        logger.info(f"Deduplicating {len(self.listings)} raw records...")
        
        # Convert to DataFrame
        records = [asdict(r) for r in self.listings]
        df = pd.DataFrame(records)
        
        if df.empty:
            logger.warning("No records to deduplicate")
            return df
        
        # Sort by timestamp descending (keep newest first)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.sort_values('timestamp', ascending=False)
        
        # Drop exact duplicates (same guid, same timestamp)
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['guid', 'timestamp'], keep='first')
        logger.info(f"Removed {before_dedup - len(df)} exact duplicates")
        
        # For each GUID, keep the most recent record as canonical
        # but preserve price history for temporal features
        canonical_df = df.drop_duplicates(subset=['guid'], keep='first').copy()
        history_df = df.copy()  # Keep full history
        
        logger.info(f"Canonical records: {len(canonical_df)}, Total history: {len(history_df)}")
        
        self.df = history_df
        self.canonical_df = canonical_df
        
        return canonical_df
    
    def extract_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Temporal Feature Extraction:
        - days_listed: latest timestamp - first timestamp
        - price_delta: latest price - initial price
        - relative_price_ratio: item price / category median price
        """
        logger.info("Extracting temporal features...")
        
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Group by GUID to calculate temporal features
        temp_features = df.groupby('guid').agg({
            'timestamp': ['min', 'max', 'count'],
            'price_kes': ['first', 'last', 'mean'],
            'category': 'first'
        }).reset_index()
        
        temp_features.columns = [
            'guid', 'first_seen', 'last_seen', 'appearance_count',
            'initial_price', 'latest_price', 'avg_price', 'category'
        ]
        
        # Calculate days listed
        temp_features['days_listed'] = (
            temp_features['last_seen'] - temp_features['first_seen']
        ).dt.days
        
        # Calculate price delta
        temp_features['price_delta'] = (
            temp_features['latest_price'] - temp_features['initial_price']
        )
        
        # Calculate relative price ratio (vs category median)
        category_medians = temp_features.groupby('category')['latest_price'].transform('median')
        temp_features['relative_price_ratio'] = (
            temp_features['latest_price'] / category_medians
        ).fillna(1.0).replace([np.inf, -np.inf], 1.0)
        
        # Merge back to main dataframe
        df = df.merge(
            temp_features[['guid', 'days_listed', 'price_delta', 'relative_price_ratio']],
            on='guid',
            how='left'
        )
        
        logger.info(f"Temporal features extracted for {len(df)} records")
        return df
    
    def normalize_currencies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all prices are normalized to KES baseline"""
        logger.info("Normalizing currencies to KES baseline...")
        
        # Recalculate price_kes if needed
        if 'price_kes' not in df.columns or df['price_kes'].isna().all():
            df['price_kes'] = df.apply(
                lambda r: r['price_original'] * CURRENCY_TO_KES.get(r.get('currency', 'KES'), 1.0),
                axis=1
            )
        
        logger.info(f"Currency normalization complete. Price range: {df['price_kes'].min():.2f} - {df['price_kes'].max():.2f} KES")
        return df
    
    def create_final_dataset(self) -> pd.DataFrame:
        """
        Create final XGBoost-ready dataset with all features merged.
        """
        if self.df is None or self.df.empty:
            logger.error("No data to process!")
            return pd.DataFrame()
        
        logger.info("Creating final XGBoost-ready dataset...")
        
        # Run full processing pipeline
        df = self.normalize_currencies(self.df)
        df = self.extract_temporal_features(df)
        
        # Select and order columns for ML readiness
        feature_columns = [
            'guid', 'title', 'price_original', 'currency', 'price_kes',
            'location', 'category', 'subcategory', 'domain', 'url',
            'timestamp', 'crawl_source', 'days_listed', 'price_delta',
            'relative_price_ratio', 'image_count', 'listing_id'
        ]
        
        # Ensure all columns exist
        for col in feature_columns:
            if col not in df.columns:
                df[col] = None
        
        final_df = df[feature_columns].copy()
        
        # Convert timestamp to unix epoch for ML
        final_df['timestamp_epoch'] = pd.to_datetime(
            final_df['timestamp'], errors='coerce'
        ).astype('int64') // 10**9
        
        # Create categorical encodings (label encoding for common fields)
        categorical_cols = ['category', 'domain', 'currency', 'crawl_source']
        for col in categorical_cols:
            if col in final_df.columns:
                final_df[f'{col}_encoded'] = final_df[col].astype('category').cat.codes
        
        logger.info(f"Final dataset shape: {final_df.shape}")
        logger.info(f"Features: {list(final_df.columns)}")
        
        return final_df
    
    def export_dataset(self, df: pd.DataFrame, format: str = 'both'):
        """Export dataset to Parquet and/or CSV"""
        if df.empty:
            logger.error("Empty dataset, nothing to export")
            return
        
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format in ['parquet', 'both']:
            parquet_path = OUTPUT_DIR / f'jiji_dataset_{timestamp}.parquet'
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, parquet_path, compression='snappy')
            logger.info(f"Exported Parquet: {parquet_path} ({parquet_path.stat().st_size / 1024 / 1024:.1f} MB)")
        
        if format in ['csv', 'both']:
            csv_path = OUTPUT_DIR / f'jiji_dataset_{timestamp}.csv'
            df.to_csv(csv_path, index=False)
            logger.info(f"Exported CSV: {csv_path} ({csv_path.stat().st_size / 1024 / 1024:.1f} MB)")
        
        # Also export schema/metadata
        metadata = {
            'extraction_timestamp': timestamp,
            'total_records': len(df),
            'columns': list(df.columns),
            'domains_covered': df['domain'].unique().tolist() if 'domain' in df.columns else [],
            'date_range': {
                'min': str(df['timestamp'].min()) if 'timestamp' in df.columns else None,
                'max': str(df['timestamp'].max()) if 'timestamp' in df.columns else None
            },
            'price_stats': {
                'mean_kes': float(df['price_kes'].mean()) if 'price_kes' in df.columns else None,
                'median_kes': float(df['price_kes'].median()) if 'price_kes' in df.columns else None,
                'min_kes': float(df['price_kes'].min()) if 'price_kes' in df.columns else None,
                'max_kes': float(df['price_kes'].max()) if 'price_kes' in df.columns else None
            }
        }
        
        meta_path = OUTPUT_DIR / f'dataset_metadata_{timestamp}.json'
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        logger.info(f"Exported metadata: {meta_path}")


class JijiExtractionPipeline:
    """
    Main orchestration class that coordinates all three paths.
    """
    
    def __init__(self, domains: List[str] = None, primary_domain: str = 'jiji.co.ke'):
        self.domains = domains or TARGET_DOMAINS
        self.primary_domain = primary_domain
        self.cc_miner = CommonCrawlMiner(max_crawls=10)
        self.wb_miner = WaybackMiner(max_workers=4, delay=1.0)
        self.processor = DataProcessor()
        self.stats = defaultdict(int)
        
    def run_common_crawl_extraction(self):
        """Execute PATH A: Common Crawl Bulk Extraction"""
        logger.info("=" * 60)
        logger.info("PATH A: Starting Common Crawl Bulk Extraction")
        logger.info("=" * 60)
        
        total_listings = []
        
        # Focus on primary domain first for maximum coverage
        for domain in [self.primary_domain] + [d for d in self.domains if d != self.primary_domain]:
            logger.info(f"\n--- Processing domain: {domain} ---")
            
            # Query index
            records = self.cc_miner.query_domain_index(domain)
            self.stats[f'cc_records_{domain}'] = len(records)
            
            if not records:
                continue
            
            # Filter to category pages
            cat_pages = self.cc_miner.filter_category_pages(records)
            self.stats[f'cc_catpages_{domain}'] = len(cat_pages)
            
            # Limit for initial run (can be increased)
            cat_pages = cat_pages[:200]
            
            # Process pages in parallel batches
            batch_size = 20
            for i in range(0, len(cat_pages), batch_size):
                batch = cat_pages[i:i+batch_size]
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = {}
                    for rec in batch:
                        future = executor.submit(
                            self._process_cc_record, rec, domain
                        )
                        futures[future] = rec
                    
                    for future in as_completed(futures, timeout=300):
                        try:
                            listings = future.result(timeout=60)
                            total_listings.extend(listings)
                        except Exception as e:
                            logger.debug(f"Batch processing error: {e}")
                
                logger.info(f"  Progress: {i+len(batch)}/{len(cat_pages)} pages, {len(total_listings)} listings so far")
                time.sleep(0.5)
        
        logger.info(f"\nCommon Crawl extraction complete: {len(total_listings)} listings")
        self.processor.add_listings(total_listings)
        self.stats['cc_total_listings'] = len(total_listings)
    
    def _process_cc_record(self, record: Dict, domain: str) -> List[ListingRecord]:
        """Process a single Common Crawl record"""
        try:
            raw_data = self.cc_miner.fetch_warc_record(record)
            if not raw_data:
                return []
            
            html = self.cc_miner.parse_warc_response(raw_data)
            if not html:
                return []
            
            timestamp = record.get('timestamp', datetime.now().isoformat())
            url = record.get('url', '')
            
            return self.cc_miner.extract_listings_from_html(html, url, timestamp, domain)
            
        except Exception as e:
            logger.debug(f"CC record processing error: {e}")
            return []
    
    def run_wayback_extraction(self):
        """Execute PATH B: Wayback Machine Mining"""
        logger.info("\n" + "=" * 60)
        logger.info("PATH B: Starting Wayback Machine Mining")
        logger.info("=" * 60)
        
        total_records = []
        
        for domain in self.domains[:3]:  # Start with top 3 domains
            logger.info(f"\n--- Processing domain: {domain} ---")
            
            try:
                cdx_records = self.wb_miner.query_cdx_api(domain, from_date='20220101')
                self.stats[f'wb_records_{domain}'] = len(cdx_records)
                total_records.extend(cdx_records)
                
            except Exception as e:
                logger.error(f"Wayback extraction failed for {domain}: {e}")
            
            time.sleep(2)  # Polite delay between domains
        
        logger.info(f"\nWayback extraction complete: {len(total_records)} CDX records")
        self.stats['wb_total_records'] = len(total_records)
        
        # Convert CDX records to listings where possible
        wb_listings = self._convert_cdx_to_listings(total_records)
        self.processor.add_listings(wb_listings)
    
    def _convert_cdx_to_listings(self, cdx_records: List[Dict]) -> List[ListingRecord]:
        """Convert CDX records to ListingRecord objects"""
        listings = []
        
        for rec in cdx_records:
            try:
                url = rec.get('url', '')
                timestamp = rec.get('timestamp', '')
                domain = rec.get('_domain', self.primary_domain)
                
                # Extract basic info from URL
                guid_match = re.search(r'-m([a-zA-Z0-9]{15,})', url)
                guid = guid_match.group(1) if guid_match else hashlib.md5(url.encode()).hexdigest()[:16]
                
                # Get category from URL
                path_parts = url.split('/')
                category = 'Unknown'
                for part in path_parts:
                    if part in ['cars', 'phones', 'electronics', 'fashion', 'property']:
                        category = part.capitalize()
                        break
                
                listings.append(ListingRecord(
                    guid=guid,
                    title='',  # Will be filled during full item fetch
                    price_original=0.0,
                    currency='KES',
                    price_kes=0.0,
                    location='',
                    category=category,
                    subcategory='',
                    domain=domain,
                    url=url,
                    timestamp=timestamp,
                    crawl_source='wayback'
                ))
                
            except Exception as e:
                continue
        
        return listings
    
    def run_post_processing(self):
        """Execute PATH C: Post-Processing & Feature Engineering"""
        logger.info("\n" + "=" * 60)
        logger.info("PATH C: Starting Post-Processing & Feature Engineering")
        logger.info("=" * 60)
        
        # Create final dataset
        final_df = self.processor.create_final_dataset()
        
        # Export
        self.processor.export_dataset(final_df, format='both')
        
        # Update stats
        self.stats['final_record_count'] = len(final_df)
        
        return final_df
    
    def run_full_pipeline(self) -> pd.DataFrame:
        """Execute complete extraction pipeline"""
        start_time = time.time()
        
        logger.info("#" * 60)
        logger.info("# JIJI.CO.KE COMPREHENSIVE DATA EXTRACTION PIPELINE")
        logger.info(f"# Started: {datetime.now().isoformat()}")
        logger.info("#" * 60)
        
        try:
            # PATH A: Common Crawl
            self.run_common_crawl_extraction()
            
            # PATH B: Wayback Machine
            self.run_wayback_extraction()
            
            # PATH C: Post-processing
            final_df = self.run_post_processing()
            
            # Summary
            elapsed = time.time() - start_time
            logger.info("\n" + "#" * 60)
            logger.info("PIPELINE COMPLETE - SUMMARY")
            logger.info("#" * 60)
            logger.info(f"Total runtime: {elapsed:.1f}s ({elapsed/60:.1f}min)")
            for k, v in self.stats.items():
                logger.info(f"  {k}: {v}")
            
            return final_df
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


def main():
    """Main entry point"""
    pipeline = JijiExtractionPipeline(
        domains=['jiji.co.ke'],  # Primary focus
        primary_domain='jiji.co.ke'
    )
    
    df = pipeline.run_full_pipeline()
    
    if df is not None and not df.empty:
        print(f"\n✅ Extraction complete! Dataset saved to: {OUTPUT_DIR}")
        print(f"   Records: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"\nFiles:")
        for f in OUTPUT_DIR.glob('*'):
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"   - {f.name} ({size_mb:.2f} MB)")
    else:
        print("\n❌ No data extracted")


if __name__ == '__main__':
    main()
