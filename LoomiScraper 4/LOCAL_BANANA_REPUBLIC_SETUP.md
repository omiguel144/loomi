# Running Banana Republic Scraper Locally

Banana Republic's website uses client-side rendering (JavaScript), which requires a headless browser to scrape. This functionality cannot run in Replit's sandboxed environment due to system library constraints, but it works perfectly on your local machine.

## Why Local-Only?

- **Replit limitation**: The Nix environment lacks graphics libraries (libgtk-3, libpango, libxcb, etc.) required by Playwright's Firefox/Chromium browsers
- **Buddha Pants**: Works in Replit (uses requests + BeautifulSoup, no browser needed)
- **Banana Republic**: Must run locally (uses Playwright to render JavaScript)

## Local Setup (macOS, Linux, Windows)

### 1. Clone or Download the Project

```bash
git clone <repo-url>
cd loomi-scraper
```

Or download the `.zip` from Replit and extract it.

### 2. Create a Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Install Playwright Browser Engine

```bash
playwright install firefox
```

(Alternatively, use `playwright install chromium` for Chromium, but Firefox is recommended)

### 5. Run the Banana Republic Scraper

```bash
python main.py bananarepublic
```

The scraper will:
1. Launch Firefox headless
2. Load the Banana Republic sweaters page
3. Scroll to trigger lazy-loading
4. Find all product links in the DOM
5. Open each product page
6. Extract product data (title, price, materials, images)
7. Save to `bananarepublic_raw.csv`

### 6. Find Your Output

The CSV file will be saved as:
```
bananarepublic_raw.csv
```

It contains these 19 columns:
- `schema_version`, `style_id`, `style_slug`, `color_id`, `color_name`
- `image_url`, `gallery_image_urls`
- `product_url`, `brand_name`, `source_site`, `product_title`
- `category`, `is_apparel`
- `price_raw`, `price`, `currency`
- `materials_raw_or_page_text`, `materials_snippet`, `scrape_status`

## Buddha Pants (Works in Replit & Locally)

You can run Buddha Pants scraping anywhere (including Replit):

```bash
python main.py buddhapants
# or (default)
python main.py
```

Output: `buddhapants_raw.csv`

## Troubleshooting

### "BrowserType.launch: Host system is missing dependencies..."

If Firefox fails to launch, install these system libraries:

**macOS** (using Homebrew):
```bash
# Usually not needed on macOS - Playwright bundles dependencies
# If issues occur, try updating Playwright:
pip install --upgrade playwright
```

**Linux (Ubuntu/Debian)**:
```bash
sudo apt-get install libxcb-shm0 libxcb1 libxcomposite1 libxdamage1 libxfixes3 \
  libgtk-3-0 libpangocairo-1.0-0 libpango-1.0-0 libatk1.0-0 libcairo-gobject2 \
  libcairo2 libgdk-pixbuf-2.0-0 libxrender1 libasound2 libdbus-1-3
```

**Windows**:
Playwright usually handles dependencies. If issues occur, reinstall:
```bash
pip uninstall playwright
pip install playwright
playwright install firefox
```

### Scraper finds 0 products

1. Check your internet connection
2. Verify the URL in `BANANA_REPUBLIC_CONFIG` is still valid
3. Banana Republic may have changed their product link structure - check the HTML manually in DevTools

### Slow performance

The scraper includes politeness delays (1-2 seconds between requests) to avoid overwhelming the server. For 30+ products, expect 2-5 minutes.

## Next Steps

1. Run locally and generate `bananarepublic_raw.csv`
2. Merge with Buddha Pants CSV: `buddhapants_raw.csv`
3. Feed combined data to your Loomi LLM processor

---

**Questions?** Check `replit.md` for architecture details, or review `main.py` for configuration options.
