# Loomi Scraper

An autonomous multi-strategy web scraper for clothing e-commerce sites, designed to extract product data and materials information for sustainable fashion analysis.

## Overview

This scraper automatically selects the best scraping strategy based on runtime environment capabilities. It helps identify clothing made from natural fibers (cotton, linen, wool, silk, hemp). It extracts product data from e-commerce sites and outputs a CSV file ready for LLM processing to determine fiber compositions and "loomi approval" status.

**Fully Supported (in Replit):**
- buddhapants.com (Shopify-based) - uses RequestsStrategy

**Local-Only Support:**
- bananarepublic.gap.com (Next.js) - requires PlaywrightStrategy with browser

**Outputs:** `buddhapants_raw.csv`, `bananarepublic_raw.csv`

## How to Use

```bash
python main.py buddhapants      # Scrape Buddha Pants (default)
python main.py bananarepublic   # Scrape Banana Republic (requires browser)
python main.py --list           # Show available sites and their strategies
python main.py --env            # Show environment capabilities
```

The scraper automatically:
- Detects if running in Replit vs local environment
- Checks if Playwright/browser is available
- Selects viable strategies and attempts in priority order
- Provides clear feedback when strategies unavailable

## CSV Output Fields (19 columns)

| Field | Description |
|-------|-------------|
| schema_version | Version of CSV schema (currently 1) |
| style_id | Unique ID for the product style |
| style_slug | URL slug from product path |
| color_id | Unique ID combining style_id + color |
| color_name | Color/print name (e.g., "Black", "Red", "Default") |
| image_url | Main product image URL (first from gallery) |
| gallery_image_urls | Pipe-separated list (up to 5) or semicolon-separated for BR |
| product_url | Direct URL to the product page |
| brand_name | Brand name (e.g., "Buddha Pants", "Banana Republic") |
| source_site | Domain extracted from base URL |
| product_title | Full product title from the page |
| category | Normalized category (pants, top, dress, short, jumpsuit, skirt, accessory) |
| is_apparel | Boolean: True for apparel, False for accessories |
| price_raw | Price as displayed on the page |
| price | Numeric price value |
| currency | Currency code (USD, EUR, etc.) |
| materials_raw_or_page_text | Full text containing fabric/materials info |
| materials_snippet | Short snippet (~180 chars) with fiber + % info |
| scrape_status | Status: "ok", "missing_price", or "missing_materials" |

## Configuration

All configurable settings are at the top of `main.py`:

### SITE_CONFIG
- `name` / `brand_name`: Site and brand identifiers
- `base_url`: The main site URL
- `collection_urls`: List of category/collection pages to scrape
- `selectors`: CSS selectors for finding elements on pages
- `output_file`: Name of the output CSV file
- `category_keywords`: Mapping of keywords to category names

### Scraping Behavior
- `REQUEST_DELAY`: Seconds between requests (default: 1.5)
- `REQUEST_TIMEOUT`: Request timeout in seconds (default: 30)
- `MAX_RETRIES`: Retry attempts for failed requests (default: 3)

### Color Words
- `COLOR_WORDS`: List of common color names for inferring color from style_slug

### Fiber Keywords
- `FIBER_KEYWORDS`: List of fiber/fabric terms for materials extraction

## Adding a New Site

With the new autonomous architecture, adding sites is simplified:

1. Create a `SiteProfile` in `site_profiles.py`:
```python
NEW_SITE = SiteProfile(
    name="New Site",
    key="newsite",
    base_url="https://example.com",
    collection_urls=["https://example.com/products"],
    output_file="newsite_raw.csv",
    strategies=[
        RequestsStrategy(request_delay=1.5),  # Preferred
        PlaywrightStrategy(),                  # Fallback
    ],
    selectors={"product_links": "a.product-card"},
    product_url_patterns=["/products/"],
    extract_function_name="default",
)
```

2. Register in `SITE_PROFILES` dict
3. Add extraction function in `main.py` and register in `get_extract_function()`
4. Run the scraper - it will auto-select viable strategy

### Strategy Types:
- **RequestsStrategy:** Static HTML + embedded JSON (Shopify, etc.)
- **PlaywrightStrategy:** Browser rendering for JS-heavy sites (Next.js, SPAs)

## Technical Details

- **Language:** Python 3.11
- **Libraries:** httpx (HTTP/2), beautifulsoup4, lxml, pandas
- **Rate Limiting:** Adaptive (starts at 0.3s, slows on 429 errors)
- **Parallel Workers:** 5 concurrent threads for product scraping
- **Robots.txt:** Respected (checks before scraping)
- **Parser Support:**
  - **Buddha Pants:** Shopify JSON variant extraction + CSS selectors
  - **Banana Republic:** Custom JSON parsing (styles + schema.org) + BR-specific selectors
- **Color Variants:** Auto-detected from Shopify JSON (Buddha Pants); default per product (Banana Republic)

## Project Structure

```
loomi-scraper/
├── main.py              # CLI entry point and extraction functions
├── scraper_engine.py    # Core architecture: EnvironmentProbe, Strategies, ScrapeManager
├── site_profiles.py     # Site configurations (SiteProfile dataclass)
├── fabric_parser.py     # Fiber/fabric text parsing utilities
├── buddhapants_raw.csv  # Output file (generated after running)
├── replit.md            # This documentation
└── pyproject.toml       # Python dependencies
```

## Architecture

The scraper uses an autonomous multi-strategy pattern:

1. **EnvironmentProbe** - Detects runtime capabilities:
   - `is_replit` - Running in Replit sandbox?
   - `playwright_available` - Playwright importable?
   - `browser_available` - Can launch headless browser?

2. **ScrapingStrategy** (base class) - Defines scraping interface:
   - `RequestsStrategy` - Uses requests library (works everywhere)
   - `PlaywrightStrategy` - Uses browser automation (local only)

3. **SiteProfile** - Declarative site configuration with ordered strategies

4. **ScrapeManager** - Orchestrates strategy selection and execution with fallback

## Replit vs Local Limitations

| Feature | Replit | Local |
|---------|--------|-------|
| Buddha Pants | ✅ Full support | ✅ Full support |
| Banana Republic | ❌ No browser available | ✅ With Playwright + Firefox |
| Export to CSV | ✅ Works | ✅ Works |
| Color variant detection | ✅ Buddha Pants only | ✅ Both sites |

**Why BR doesn't work in Replit:**
- BR uses Next.js with client-side JavaScript rendering
- Product links are generated dynamically and don't exist in initial HTML
- Replit sandbox lacks system libraries (GTK, libxcb) needed for headless browser
- **Solution:** Run `python main.py buddhapants` in Replit (works perfectly), or run locally with Playwright

## Recent Changes

- **2025-11-29**: Performance optimizations for faster scraping
  - Replaced requests with httpx for HTTP/2 support and better connection pooling
  - Added ThreadPoolExecutor with 5 concurrent workers for parallel product scraping
  - Implemented adaptive rate limiting (0.3s start, auto-slows on 429 errors)
  - Added skip-already-scraped logic by loading existing URLs from CSV
  - Skipped expensive color analysis (dominant_hex) - deferred to post-processing
  - Buddha Pants now scrapes ~22 products (359 variants) in ~1 minute vs ~5 minutes before

- **2025-11-29**: Autonomous multi-strategy scraper architecture
  - Created `scraper_engine.py` with EnvironmentProbe, Strategy classes, ScrapeManager
  - Created `site_profiles.py` for pluggable site configuration
  - Auto-detects environment capabilities and selects viable strategies
  - Clear user feedback when strategies unavailable (e.g., BR in Replit)
  - CLI commands: `--list` (show sites), `--env` (show capabilities)
  - Buddha Pants uses RequestsStrategy, Banana Republic uses PlaywrightStrategy

- **2025-11-29**: Banana Republic scraper limitations and cleanup
  - Identified that BR uses Next.js (client-side rendering) - product links not available in static HTML
  - Added graceful fallback message when running `python main.py bananarepublic` in Replit
  - Fixed type annotation errors in URL parsing for both Buddha Pants and BR code paths
  - Buddha Pants remains fully functional: extracts 182 rows with color variants from 37 products
  - BR can still be used locally where Playwright with browser libraries are available

- **2025-11-29**: Fixed multi-color variant extraction for Buddha Pants
  - Fixed `parse_fabric_breakdown()` tuple unpacking issue (returns `(breakdown, tags)`, not dict)
  - Fixed `featured_image['src']` handling when it contains a dict with 'url' key (Schema.org format)
  - Now properly extracts 182 rows from 37 products (all products have multiple color variants)
  - Example: One product now outputs 7 color variants (Beige, Black, Green, Orange, Purple, Red, Turquoise)

- **2025-11-27**: Added Banana Republic (bananarepublic.gap.com) first-class support
  - Custom JSON parser for BR's "styles" object structure
  - Schema.org JSON extraction for price/currency
  - BR-specific image extraction from pdp-photo-single-column-image divs
  - CLI argument to switch between sites: `python main.py [buddhapants|bananarepublic]`
  - All outputs in unified 19-column format for easy merging

- **2025-11-27**: Enhanced Buddha Pants scraper with improvements
  - Added schema versioning (schema_version=1) for future compatibility
  - Implemented deduplication by style_id + color_name with stable sorting
  - Added scrape_status tracking (ok, missing_price, missing_materials)
  - Normalized categories and capitalized color names
  - Added is_apparel boolean flag to distinguish clothing from accessories
  - 19-column CSV ready for Loomi LLM processing

## Replit vs Local Limitations

| Feature | Replit | Local |
|---------|--------|-------|
| Buddha Pants | ✅ Full support | ✅ Full support |
| Banana Republic | ❌ No browser available | ✅ With Playwright + Firefox |
| Export to CSV | ✅ Works | ✅ Works |
| Color variant detection | ✅ Buddha Pants only | ✅ Both sites |

**Why BR doesn't work in Replit:**
- BR uses Next.js with client-side JavaScript rendering
- Product links are generated dynamically and don't exist in initial HTML
- Replit sandbox lacks system libraries (GTK, libxcb) needed for headless browser
- **Solution:** Run `python main.py buddhapants` in Replit (works perfectly), or run locally with Playwright

## Next Steps (Future Enhancements)

- BR API integration (if Gap provides one)
- JSON/YAML external config file for managing multiple sites without code changes
- Resume capability for interrupted scraping sessions
- Site-specific scraper classes for better organization
- Support for additional retailers (ASOS, Everlane, Patagonia, etc.)
- LLM integration to classify fiber content and determine "loomi approval"
