"""
Scraper Engine - Autonomous Multi-Strategy Scraping Architecture
=================================================================
Provides environment-aware scraping with automatic strategy selection.

Components:
- EnvironmentProbe: Detects runtime capabilities (Replit, Playwright, etc.)
- Strategy classes: RequestsStrategy, PlaywrightStrategy, ApiStrategy
- SiteProfile: Configuration for each site with strategy priorities
- ScrapeManager: Orchestrator that selects and runs viable strategies

Optimizations:
- ThreadPoolExecutor for parallel product scraping (5 workers)
- httpx for HTTP/2 support and better connection pooling
- Adaptive rate limiting (starts fast, slows on 429s)
- Skip already-scraped products from existing CSV
"""

import os
import csv
import logging
import time
import random
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable, Set, TYPE_CHECKING
from urllib.parse import urljoin
from threading import Lock

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_WORKERS = 5
DEFAULT_MIN_DELAY = 0.3
DEFAULT_MAX_DELAY = 2.0
DEFAULT_TIMEOUT = 30


# =============================================================================
# ADAPTIVE RATE LIMITER
# =============================================================================

class AdaptiveRateLimiter:
    """Adjusts request delays based on server responses."""
    
    def __init__(self, min_delay: float = 0.3, max_delay: float = 2.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.current_delay = min_delay
        self.consecutive_successes = 0
        self.lock = Lock()
    
    def record_success(self):
        """Record a successful request, potentially speeding up."""
        with self.lock:
            self.consecutive_successes += 1
            if self.consecutive_successes >= 10:
                self.current_delay = max(self.min_delay, self.current_delay * 0.9)
                self.consecutive_successes = 0
    
    def record_rate_limit(self):
        """Record a rate limit hit, slowing down."""
        with self.lock:
            self.current_delay = min(self.max_delay, self.current_delay * 2)
            self.consecutive_successes = 0
            logger.warning(f"Rate limit detected, slowing to {self.current_delay:.1f}s delay")
    
    def record_error(self):
        """Record an error, slightly slowing down."""
        with self.lock:
            self.current_delay = min(self.max_delay, self.current_delay * 1.2)
            self.consecutive_successes = 0
    
    def wait(self):
        """Wait for the current delay with some jitter."""
        with self.lock:
            delay = self.current_delay
        jitter = random.uniform(0, delay * 0.3)
        time.sleep(delay + jitter)
    
    def get_delay(self) -> float:
        """Get current delay value."""
        with self.lock:
            return self.current_delay


# =============================================================================
# ENVIRONMENT PROBE
# =============================================================================

class EnvironmentProbe:
    """Detects runtime environment and available capabilities."""
    
    def __init__(self):
        self._playwright_available = None
        self._browser_available = None
    
    @property
    def is_replit(self) -> bool:
        """Check if running in Replit environment."""
        return bool(os.environ.get("REPL_ID"))
    
    @property
    def playwright_available(self) -> bool:
        """Check if Playwright is installed and importable."""
        if self._playwright_available is None:
            try:
                from playwright.sync_api import sync_playwright
                self._playwright_available = True
            except ImportError:
                self._playwright_available = False
        return self._playwright_available
    
    @property
    def browser_available(self) -> bool:
        """Check if a browser can actually be launched (not just installed)."""
        if self._browser_available is None:
            if not self.playwright_available:
                self._browser_available = False
            elif self.is_replit:
                self._browser_available = False
            else:
                try:
                    from playwright.sync_api import sync_playwright
                    with sync_playwright() as p:
                        browser = p.firefox.launch(headless=True)
                        browser.close()
                    self._browser_available = True
                except Exception as e:
                    logger.debug(f"Browser check failed: {e}")
                    self._browser_available = False
        return self._browser_available
    
    def has_secret(self, key: str) -> bool:
        """Check if a specific secret/API key is available."""
        return bool(os.environ.get(key))
    
    def get_capabilities(self) -> Dict[str, bool]:
        """Return a summary of all detected capabilities."""
        return {
            "is_replit": self.is_replit,
            "playwright_available": self.playwright_available,
            "browser_available": self.browser_available,
        }
    
    def __repr__(self):
        caps = self.get_capabilities()
        return f"EnvironmentProbe({caps})"


# =============================================================================
# STRATEGY BASE CLASS
# =============================================================================

class ScrapingStrategy(ABC):
    """Base class for all scraping strategies."""
    
    name: str = "base"
    requires_browser: bool = False
    requires_api_key: Optional[str] = None
    
    @abstractmethod
    def supports(self, env: EnvironmentProbe) -> bool:
        """Check if this strategy can run in the current environment."""
        pass
    
    @abstractmethod
    def scrape_collection(self, client: httpx.Client, collection_url: str, 
                          config: Dict[str, Any]) -> set:
        """Extract product URLs from a collection/category page."""
        pass
    
    @abstractmethod
    def scrape_product(self, client: httpx.Client, product_url: str,
                       config: Dict[str, Any], extract_fn: Callable,
                       rate_limiter: AdaptiveRateLimiter) -> List[Dict]:
        """Scrape a single product page and return records."""
        pass
    
    def get_status_message(self, env: EnvironmentProbe) -> str:
        """Return a user-friendly message about this strategy's availability."""
        if self.supports(env):
            return f"✓ {self.name} strategy available"
        return f"✗ {self.name} strategy not available"
    
    def cleanup(self) -> None:
        """Clean up any resources (override in subclasses)."""
        pass


# =============================================================================
# REQUESTS STRATEGY (Static HTML + embedded JSON) - Now using httpx
# =============================================================================

class RequestsStrategy(ScrapingStrategy):
    """Scrapes using httpx library - works with static HTML and embedded JSON."""
    
    name = "requests"
    requires_browser = False
    
    def __init__(self, request_delay: float = 0.3):
        self.request_delay = request_delay
    
    def supports(self, env: EnvironmentProbe) -> bool:
        """Always supported - just needs internet access."""
        return True
    
    def scrape_collection(self, client: httpx.Client, collection_url: str,
                          config: Dict[str, Any]) -> set:
        """Extract product links from collection page using httpx."""
        product_urls = set()
        
        try:
            resp = client.get(collection_url, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')
            
            selectors = config.get("selectors", {}).get("product_links", "")
            if isinstance(selectors, str):
                selectors = [s.strip() for s in selectors.split(",")]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    for el in elements:
                        href = el.get('href', '')
                        if isinstance(href, str) and href:
                            full_url = urljoin(config["base_url"], href)
                            if self._is_product_url(full_url, config):
                                product_urls.add(full_url)
                    if product_urls:
                        break
            
            time.sleep(self.request_delay)
            
        except Exception as e:
            logger.warning(f"RequestsStrategy error on {collection_url}: {e}")
        
        return product_urls
    
    def scrape_product(self, client: httpx.Client, product_url: str,
                       config: Dict[str, Any], extract_fn: Callable,
                       rate_limiter: AdaptiveRateLimiter) -> List[Dict]:
        """Scrape product page using httpx with adaptive rate limiting and retries."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                resp = client.get(product_url, timeout=DEFAULT_TIMEOUT)
                
                if resp.status_code == 429:
                    rate_limiter.record_rate_limit()
                    rate_limiter.wait()
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return []
                
                resp.raise_for_status()
                rate_limiter.record_success()
                
                html = resp.text
                soup = BeautifulSoup(html, 'lxml')
                
                records = extract_fn(html, soup, product_url, config)
                
                # Only wait if we got data (success)
                if records:
                    rate_limiter.wait()
                
                return records
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    rate_limiter.record_rate_limit()
                elif e.response.status_code >= 500 and attempt < max_retries - 1:
                    # Retry on server errors
                    logger.debug(f"Server error, retry {attempt + 1}/{max_retries}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    rate_limiter.record_error()
                logger.warning(f"RequestsStrategy error on {product_url}: {e}")
                return []
            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    logger.debug(f"Timeout, retry {attempt + 1}/{max_retries}")
                    continue
                logger.warning(f"RequestsStrategy timeout on {product_url}")
                return []
            except Exception as e:
                rate_limiter.record_error()
                logger.warning(f"RequestsStrategy error on {product_url}: {e}")
                return []
        
        return []
    
    def _is_product_url(self, url: str, config: Dict[str, Any]) -> bool:
        """Check if URL looks like a product page."""
        product_patterns = config.get("product_url_patterns", ["/products/", "/p/", "pid="])
        return any(pattern in url for pattern in product_patterns)


# =============================================================================
# PLAYWRIGHT STRATEGY (Browser rendering for JS-heavy sites)
# =============================================================================

class PlaywrightStrategy(ScrapingStrategy):
    """Scrapes using Playwright browser - for JavaScript-rendered sites."""
    
    name = "playwright"
    requires_browser = True
    
    def __init__(self, scroll_count: int = 10, page_timeout: int = 60000):
        self.scroll_count = scroll_count
        self.page_timeout = page_timeout
        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._page: Any = None
    
    def supports(self, env: EnvironmentProbe) -> bool:
        """Requires Playwright installed and browser available."""
        return env.browser_available
    
    def _ensure_browser(self):
        """Lazy-initialize browser on first use."""
        if self._browser is None:
            from playwright.sync_api import sync_playwright
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.firefox.launch(headless=True)
            self._context = self._browser.new_context()
            self._page = self._context.new_page()
    
    def cleanup(self) -> None:
        """Clean up browser resources."""
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        self._browser = None
        self._context = None
        self._page = None
    
    def scrape_collection(self, client: httpx.Client, collection_url: str,
                          config: Dict[str, Any]) -> set:
        """Extract product links from collection page using Playwright."""
        product_urls = set()
        
        try:
            self._ensure_browser()
            self._page.goto(collection_url, wait_until="domcontentloaded", 
                           timeout=self.page_timeout)
            
            selector = config.get("selectors", {}).get("product_links", "a")
            if "," in selector:
                selector = selector.split(",")[0].strip()
            
            last_count = 0
            for i in range(self.scroll_count):
                self._page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                time.sleep(1)
                
                links = self._page.query_selector_all(selector)
                if len(links) == last_count:
                    break
                last_count = len(links)
            
            links = self._page.query_selector_all(selector)
            for link in links:
                href = link.get_attribute('href')
                if href:
                    full_url = urljoin(config["base_url"], href)
                    if self._is_product_url(full_url, config):
                        product_urls.add(full_url)
            
        except Exception as e:
            logger.warning(f"PlaywrightStrategy error on {collection_url}: {e}")
        
        return product_urls
    
    def scrape_product(self, client: httpx.Client, product_url: str,
                       config: Dict[str, Any], extract_fn: Callable,
                       rate_limiter: AdaptiveRateLimiter) -> List[Dict]:
        """Scrape product page using Playwright."""
        try:
            self._ensure_browser()
            self._page.goto(product_url, wait_until="domcontentloaded",
                           timeout=self.page_timeout)
            self._page.wait_for_timeout(3000)
            
            html = self._page.content()
            soup = BeautifulSoup(html, 'lxml')
            
            records = extract_fn(html, soup, product_url, config)
            
            rate_limiter.wait()
            
            return records
            
        except Exception as e:
            logger.warning(f"PlaywrightStrategy error on {product_url}: {e}")
            return []
    
    def _is_product_url(self, url: str, config: Dict[str, Any]) -> bool:
        """Check if URL looks like a product page."""
        product_patterns = config.get("product_url_patterns", 
                                       ["/browse/product", "/p/", "pid="])
        return any(pattern in url for pattern in product_patterns)


# =============================================================================
# SITE PROFILE
# =============================================================================

@dataclass
class SiteProfile:
    """Configuration for a single e-commerce site."""
    
    name: str
    key: str
    base_url: str
    collection_urls: List[str]
    output_file: str
    
    strategies: List[ScrapingStrategy] = field(default_factory=list)
    
    selectors: Dict[str, str] = field(default_factory=dict)
    product_url_patterns: List[str] = field(default_factory=list)
    
    brand_name: str = ""
    source_site: str = ""
    
    extract_function_name: str = "default"
    
    def get_config(self) -> Dict[str, Any]:
        """Convert to legacy config dict format for compatibility."""
        return {
            "name": self.name,
            "base_url": self.base_url,
            "collection_urls": self.collection_urls,
            "output_file": self.output_file,
            "selectors": self.selectors,
            "product_url_patterns": self.product_url_patterns,
            "brand_name": self.brand_name or self.name,
            "source_site": self.source_site or self.base_url.replace("https://", "").replace("http://", "").rstrip("/"),
        }


# =============================================================================
# SCRAPE MANAGER (with parallel processing and optimizations)
# =============================================================================

class ScrapeManager:
    """Orchestrates scraping with automatic strategy selection and parallel execution."""
    
    def __init__(self, site_profile: SiteProfile, extract_fn: Callable, 
                 max_workers: int = DEFAULT_WORKERS, skip_existing: bool = True,
                 incremental: bool = True, save_interval: int = 25):
        self.site = site_profile
        self.extract_fn = extract_fn
        self.max_workers = max_workers
        self.skip_existing = skip_existing
        self.incremental = incremental
        self.save_interval = save_interval
        
        self.env = EnvironmentProbe()
        self.client = self._create_client()
        self.rate_limiter = AdaptiveRateLimiter()
        
        self.all_records: List[Dict] = []
        self.failed_products: int = 0
        self.skipped_products: int = 0
        self.strategy_used: Optional[ScrapingStrategy] = None
        self.existing_urls: Set[str] = set()
        
        self._records_lock = Lock()
        self._products_processed: int = 0
    
    def _create_client(self) -> httpx.Client:
        """Create an httpx client with HTTP/2 support."""
        return httpx.Client(
            http2=True,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            },
            follow_redirects=True,
            timeout=DEFAULT_TIMEOUT
        )
    
    def _load_existing_urls(self) -> Set[str]:
        """Load already-scraped product URLs from existing CSV."""
        existing = set()
        output_file = self.site.output_file
        
        if not os.path.exists(output_file):
            return existing
        
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get('product_url', '')
                    if url:
                        existing.add(url)
            logger.info(f"Loaded {len(existing)} existing URLs from {output_file}")
        except Exception as e:
            logger.warning(f"Could not load existing URLs: {e}")
        
        return existing
    
    def get_viable_strategies(self) -> List[ScrapingStrategy]:
        """Return strategies that can run in current environment, in priority order."""
        viable = []
        for strategy in self.site.strategies:
            if strategy.supports(self.env):
                viable.append(strategy)
            else:
                logger.info(f"  Strategy '{strategy.name}' not available: {strategy.get_status_message(self.env)}")
        return viable
    
    def run(self) -> List[Dict]:
        """Execute scraping with automatic strategy selection and parallel processing."""
        logger.info("=" * 70)
        logger.info(f"Starting Loomi Scraper for {self.site.name}")
        logger.info("=" * 70)
        
        logger.info(f"\nEnvironment: {'Replit' if self.env.is_replit else 'Local'}")
        logger.info(f"Playwright available: {self.env.playwright_available}")
        logger.info(f"Browser available: {self.env.browser_available}")
        logger.info(f"Parallel workers: {self.max_workers}")
        
        if self.skip_existing:
            self.existing_urls = self._load_existing_urls()
        
        viable_strategies = self.get_viable_strategies()
        
        if not viable_strategies:
            self._show_no_strategy_message()
            return []
        
        logger.info(f"\nViable strategies: {[s.name for s in viable_strategies]}")
        
        strategy_attempted = False
        for strategy in viable_strategies:
            logger.info(f"\n>>> Attempting strategy: {strategy.name}")
            strategy_attempted = True
            
            try:
                records = self._run_with_strategy(strategy)
                
                if records:
                    self.strategy_used = strategy
                    self.all_records = records
                    logger.info(f"\n✓ Strategy '{strategy.name}' succeeded with {len(records)} records")
                    break
                else:
                    logger.warning(f"Strategy '{strategy.name}' returned no records, trying next...")
                    
            except Exception as e:
                logger.error(f"Strategy '{strategy.name}' failed: {e}")
                continue
            finally:
                strategy.cleanup()
        
        if not strategy_attempted or not self.all_records:
            self._show_failure_summary(viable_strategies)
        
        self._print_summary()
        self.client.close()
        return self.all_records
    
    def _scrape_single_product(self, strategy: ScrapingStrategy, product_url: str, 
                                config: Dict[str, Any], index: int, total: int) -> List[Dict]:
        """Scrape a single product (called by worker threads)."""
        try:
            logger.info(f"[{index}/{total}] Scraping: {product_url[:60]}...")
            
            product_records = strategy.scrape_product(
                self.client, product_url, config, self.extract_fn, self.rate_limiter
            )
            
            if product_records:
                with self._records_lock:
                    r = product_records[0]
                    logger.info(f"  ✓ {len(product_records)} variants: {r.get('color_name', 'N/A')}")
                    self._products_processed += 1
                    
                    # Incremental save every N products
                    if self.incremental and self._products_processed % self.save_interval == 0:
                        self._incremental_save()
                
                return product_records
            else:
                with self._records_lock:
                    self.failed_products += 1
                logger.warning(f"  ✗ No records extracted")
                return []
                
        except Exception as e:
            with self._records_lock:
                self.failed_products += 1
            logger.error(f"  ✗ Error: {e}")
            return []
    
    def _incremental_save(self):
        """Save progress incrementally (called with lock held)."""
        if not self.all_records:
            return
        
        temp_file = f"{self.site.output_file}.partial"
        try:
            # Import standardize and save functions
            from main import standardize_record, CSV_FIELDNAMES
            import csv
            
            standardized = [standardize_record(r) for r in self.all_records]
            with open(temp_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                writer.writeheader()
                writer.writerows(standardized)
            
            logger.info(f"  💾 Saved {len(self.all_records)} records to {temp_file}")
        except Exception as e:
            logger.warning(f"  Incremental save failed: {e}")
    
    def _run_with_strategy(self, strategy: ScrapingStrategy) -> List[Dict]:
        """Run scraping using a specific strategy with parallel processing."""
        config = self.site.get_config()
        all_product_urls = set()
        
        for collection_url in self.site.collection_urls:
            logger.info(f"\nProcessing collection: {collection_url}")
            urls = strategy.scrape_collection(self.client, collection_url, config)
            logger.info(f"  Found {len(urls)} products")
            all_product_urls.update(urls)
            
            if len(all_product_urls) >= 100:
                logger.info("  Reached product limit (100)")
                break
        
        if self.skip_existing and self.existing_urls:
            before_count = len(all_product_urls)
            all_product_urls = all_product_urls - self.existing_urls
            self.skipped_products = before_count - len(all_product_urls)
            if self.skipped_products > 0:
                logger.info(f"\nSkipping {self.skipped_products} already-scraped products")
        
        logger.info(f"\nTotal products to scrape: {len(all_product_urls)}")
        
        if not all_product_urls:
            return []
        
        product_urls = sorted(all_product_urls)
        total = len(product_urls)
        records = []
        
        if strategy.requires_browser:
            for i, url in enumerate(product_urls, 1):
                result = self._scrape_single_product(strategy, url, config, i, total)
                records.extend(result)
        else:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(
                        self._scrape_single_product, strategy, url, config, i, total
                    ): url
                    for i, url in enumerate(product_urls, 1)
                }
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        records.extend(result)
                    except Exception as e:
                        logger.error(f"Worker error: {e}")
        
        return records
    
    def _show_no_strategy_message(self):
        """Display helpful message when no strategies are available."""
        print("\n" + "=" * 70)
        print(f"⚠️  No viable scraping strategy for {self.site.name}")
        print("=" * 70)
        self._show_environment_limitations()
        print("\nSuggested actions:")
        print("  • Try a different site that supports requests-only scraping")
        print("  • Run locally with Playwright installed")
        print("=" * 70 + "\n")
    
    def _show_environment_limitations(self):
        """Display environment limitations affecting scraping."""
        print(f"\nConfigured strategies: {[s.name for s in self.site.strategies]}")
        print(f"\nEnvironment limitations:")
        if self.env.is_replit:
            print("  • Running in Replit (browser-based strategies unavailable)")
        if not self.env.playwright_available:
            print("  • Playwright not installed")
        elif not self.env.browser_available:
            print("  • Browser cannot be launched (missing system libraries)")
    
    def _show_failure_summary(self, viable_strategies: List[ScrapingStrategy]):
        """Display summary when all strategies failed."""
        print("\n" + "=" * 70)
        print(f"⚠️  All strategies failed for {self.site.name}")
        print("=" * 70)
        if not viable_strategies:
            self._show_environment_limitations()
        else:
            print(f"\nAttempted strategies: {[s.name for s in viable_strategies]}")
            print("All returned zero records.")
        print("\nFor debugging:")
        print("  • Check the log output above for specific errors")
        print("  • Run 'python main.py --env' to see environment capabilities")
        print("=" * 70 + "\n")
    
    def _print_summary(self):
        """Print scraping summary."""
        logger.info("\n" + "=" * 70)
        logger.info("SCRAPING COMPLETE")
        logger.info(f"Strategy used: {self.strategy_used.name if self.strategy_used else 'None'}")
        logger.info(f"Total records: {len(self.all_records)}")
        logger.info(f"Failed products: {self.failed_products}")
        logger.info(f"Skipped (already scraped): {self.skipped_products}")
        logger.info(f"Current delay: {self.rate_limiter.get_delay():.2f}s")
        logger.info(f"Output file: {self.site.output_file}")
        logger.info("=" * 70)
