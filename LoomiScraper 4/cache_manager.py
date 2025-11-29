
"""
Cache Manager - Intelligent caching for scraped product data
=============================================================
Tracks product checksums to avoid re-scraping unchanged pages.
"""

import json
import hashlib
import os
from typing import Dict, Optional
from datetime import datetime, timedelta

class CacheManager:
    """Manages product page checksums to detect changes."""
    
    def __init__(self, cache_file: str = ".scraper_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()
    
    def _load_cache(self) -> Dict:
        """Load cache from disk."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {"products": {}, "last_updated": None}
    
    def _save_cache(self):
        """Save cache to disk."""
        self.cache["last_updated"] = datetime.now().isoformat()
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
    
    def get_checksum(self, html: str) -> str:
        """Generate checksum for HTML content."""
        # Hash only the product data section to avoid false changes from ads/banners
        return hashlib.md5(html.encode('utf-8')).hexdigest()
    
    def is_changed(self, product_url: str, html: str) -> bool:
        """Check if product page has changed since last scrape."""
        current_checksum = self.get_checksum(html)
        cached = self.cache["products"].get(product_url, {})
        
        if not cached or cached.get("checksum") != current_checksum:
            # Update cache
            self.cache["products"][product_url] = {
                "checksum": current_checksum,
                "last_scraped": datetime.now().isoformat()
            }
            self._save_cache()
            return True
        
        return False
    
    def should_refresh(self, product_url: str, max_age_days: int = 7) -> bool:
        """Check if cached product should be refreshed based on age."""
        cached = self.cache["products"].get(product_url, {})
        if not cached:
            return True
        
        last_scraped = datetime.fromisoformat(cached["last_scraped"])
        age = datetime.now() - last_scraped
        return age > timedelta(days=max_age_days)
