#!/usr/bin/env python3
"""
Loomi Scraper - Clothing E-commerce Product Scraper with Color Variants
=========================================================================
Extracts product data including color/print variants with all gallery images
from clothing e-commerce sites. Outputs one CSV row per color variant.

Currently configured for buddhapants.com but configurable for other retailers.

Usage: python main.py
Output: CSV file with product color variant data
"""

import csv
import re
import json
import time
import logging
import hashlib
import sys
import os
import random
from urllib.parse import urljoin, urlparse, parse_qs
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from colorthief import ColorThief
import webcolors
from io import BytesIO
from fabric_parser import parse_fabric_breakdown

# =============================================================================
# CONFIGURATION - Modify these settings for different sites
# =============================================================================

SITE_CONFIG = {
    "name": "Buddha Pants",
    "brand_name": "Buddha Pants",
    "base_url": "https://www.buddhapants.com",
    
    # Collection/category pages to scrape
    "collection_urls": [
        "https://www.buddhapants.com/collections/harem-pants",
        "https://www.buddhapants.com/collections/yoga-pants",
        "https://www.buddhapants.com/collections/jumpsuits",
        "https://www.buddhapants.com/collections/tops",
        "https://www.buddhapants.com/collections/dresses",
    ],
    
    # CSS Selectors - adjust for different site structures
    "selectors": {
        # Listing page selectors
        "product_link": "a.product-card, .product-item a, .product-grid-item a, a[href*='/products/']",
        "pagination_next": "a.pagination__next, .pagination a[rel='next'], a[aria-label='Next page']",
        
        # Product detail page selectors
        "product_title": "h1.product__title, h1.product-title, h1[class*='title'], .product-single__title, h1",
        "price": ".product__price, .price, .product-price, .money, [class*='price'] .money, .product-single__price",
        "main_image": ".product__media img, .product-featured-image img, .product-single__photo img, .product-image img, img[class*='product']",
        "gallery_images": ".product__media img, .product-gallery img, .gallery img, .product-image img",
        "description": ".product__description, .product-description, .product-single__description, [class*='description']",
        "materials": ".product__description, .product-description, #product-description, .product-single__description, .accordion__content, [class*='material'], [class*='fabric'], .product-details",
        
        # Color variant selectors
        "color_swatches": ".product-options .swatch, [class*='color'] .swatch, .variant-swatch, .product-variant, [class*='variant']",
        "color_name": "[class*='color-name'], [class*='color-label'], .swatch-label, [data-color], .variant-name",
    },
    
    # Output settings
    "output_file": "buddhapants_raw.csv",
    
    # Category detection keywords
    "category_keywords": {
        "jumpsuits": ["jumpsuit", "jumper", "romper", "onesie", "catsuit"],
        "dresses": ["dress", "gown"],
        "accessories": ["bag", "purse", "fanny", "clutch", "scarf", "sticker", "journal", "notes", "hat", "lanyard"],
        "tops": ["top", "shirt", "blouse", "tee", "tank", "hoodie", "sweater"],
        "shorts": ["short", "capri"],
        "pants": ["pant", "harem", "yoga-pant", "trouser", "jogger"],
        "skirts": ["skirt"],
    },
}

# Banana Republic configuration (using bananarepublic.gap.com)
BANANA_REPUBLIC_CONFIG = {
    "name": "Banana Republic",
    "brand_name": "Banana Republic",
    "base_url": "https://bananarepublic.gap.com",
    "parser_type": "banana_republic",  # Special flag for BR-specific JSON parsing
    
    # Collection/category pages to scrape
    "collection_urls": [
          # Women complete catalog + major garment types
          "https://bananarepublic.gap.com/browse/women?cid=5002",
          "https://bananarepublic.gap.com/browse/women/dresses-and-jumpsuits?cid=69883",
          "https://bananarepublic.gap.com/browse/women/pants?cid=67595",
          "https://bananarepublic.gap.com/browse/women/sweaters?cid=5032",
          "https://bananarepublic.gap.com/browse/women/workwear?cid=1178917",
          "https://bananarepublic.gap.com/browse/women/accessories?cid=1134528",

          # Men complete catalog (includes sweaters, pants, etc.)
          "https://bananarepublic.gap.com/browse/men?cid=5343",
    ],
    
    # CSS Selectors - Banana Republic specific
    "selectors": {
        # Listing page selectors
        "product_link": "a[href*='/browse/product.do?pid='], .productCarousel a",
        "pagination_next": "a.nextPage, .nextPage",
        
        # Product detail page selectors (for fallback CSS parsing)
        "product_title": "h1, span[class*='productName']",
        "price": ".productPrice, [class*='price']",
        "main_image": "div.pdp-photo-single-column-image img",
        "gallery_images": "div.pdp-photo-single-column-image",
        "description": ".productDescription, [class*='description']",
        "materials": ".productDetails, [class*='fabric'], [class*='material']",
        
        # Color variant selectors
        "color_swatches": ".colorOption, [class*='color'] button",
        "color_name": "[data-colorname], .colorName",
    },
    
    # Output settings
    "output_file": "bananarepublic_raw.csv",
    
    # Color extraction settings
    "use_hex_fallback": True,  # Use hex color analysis when DOM extraction fails
    
    # Category detection keywords
    "category_keywords": {
        "jumpsuits": ["jumpsuit", "romper"],
        "dresses": ["dress", "gown"],
        "accessories": ["bag", "purse", "scarf", "hat", "shoe", "belt", "glove", "sock"],
        "tops": ["top", "shirt", "blouse", "tee", "tank", "sweater", "cardigan", "jacket", "blazer", "sweater"],
        "shorts": ["short", "capri"],
        "pants": ["pant", "trouser", "jean", "jogger", "legging", "chino"],
        "skirts": ["skirt"],
    },
}

# =============================================================================
# SITE CONFIGS MAPPING - Map site names to their configurations
# =============================================================================

SITE_CONFIGS = {
    "buddhapants": SITE_CONFIG,
    "bananarepublic": BANANA_REPUBLIC_CONFIG,
}

# Scraping behavior settings
REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

USER_AGENT = "LoomiBotScraper/1.0 (Sustainable Fashion Research; Contact: hello@example.com)"

# Fiber words for materials extraction
FIBER_KEYWORDS = ['cotton', 'polyester', 'rayon', 'linen', 'silk', 'wool', 'hemp', 'bamboo', 'viscose', 'spandex', 'nylon', 'acrylic', 'elastane', 'lycra', 'cashmere']

# Common color words for inferring color from style_slug
COLOR_WORDS = ['black', 'white', 'red', 'blue', 'green', 'navy', 'olive', 'pink', 'grey', 'gray', 'brown', 'beige', 'tan', 'orange', 'yellow', 'purple', 'maroon', 'burgundy', 'cream', 'ivory', 'charcoal', 'teal', 'coral', 'mint', 'sage', 'lavender', 'plum', 'indigo', 'khaki', 'mustard', 'rust', 'wine', 'blush', 'nude', 'taupe', 'slate', 'mauve', 'turquoise', 'fuchsia', 'magenta', 'silver', 'gold', 'rose', 'sand', 'camel', 'mocha', 'espresso', 'chocolate', 'midnight', 'pewter']

# =============================================================================
# CSV FIELD NAMES - 19 columns (added schema_version and scrape_status)
# =============================================================================

SCHEMA_VERSION = 1

CSV_FIELDS = [
    "schema_version",
    "style_id",
    "style_slug",
    "color_id",
    "color_name",
    "image_url",
    "gallery_image_urls",
    "product_url",
    "brand_name",
    "source_site",
    "product_title",
    "category",
    "is_apparel",
    "price_raw",
    "price",
    "currency",
    "materials_raw_or_page_text",
    "materials_snippet",
    "scrape_status",
    "fabric_breakdown_pretty",
    "fabric_tags",
    "occasion_tag",
]

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# =============================================================================
# FABRIC-AWARE HELPERS - Fiber composition analysis
# =============================================================================

def classify_occasion(title: str, category: str):
    """Classify product occasion based on title and category."""
    text = f"{title or ''} {category or ''}".lower()

    work_words = ["work", "office", "blazer", "trouser", "meeting", "suit"]
    event_words = ["party", "event", "evening", "wedding", "gala", "cocktail"]
    offduty_words = ["weekend", "relaxed", "lounge", "travel", "vacation", "casual", "yoga"]

    score_work = sum(w in text for w in work_words)
    score_event = sum(w in text for w in event_words)
    score_off = sum(w in text for w in offduty_words)

    if score_work >= score_event and score_work >= score_off and score_work > 0:
        return "Work & Meetings"
    if score_event >= score_work and score_event >= score_off and score_event > 0:
        return "Events & Dinners"
    if score_off > 0:
        return "Off-Duty & Travel"

    return "Off-Duty & Travel"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_session():
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    return session


def check_robots_txt(base_url):
    """Check robots.txt and return a RobotFileParser."""
    rp = RobotFileParser()
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        rp.set_url(robots_url)
        rp.read()
        logger.info(f"Successfully read robots.txt")
    except Exception as e:
        logger.warning(f"Could not read robots.txt: {e}")
    return rp


def can_fetch(robot_parser, url):
    """Check if we're allowed to fetch a URL according to robots.txt."""
    try:
        return robot_parser.can_fetch(USER_AGENT, url)
    except Exception:
        return True


def fetch_page(session, url, robot_parser=None):
    """Fetch a page with retry logic and rate limiting."""
    if robot_parser and not can_fetch(robot_parser, url):
        logger.warning(f"Robots.txt disallows: {url}")
        return None
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(REQUEST_DELAY * 2)
    
    logger.error(f"Failed to fetch {url}")
    return None


def detect_category(url, title):
    """Detect product category based on URL and title."""
    text = f"{url} {title}".lower()
    for category, keywords in SITE_CONFIG["category_keywords"].items():
        for keyword in keywords:
            if keyword in text:
                return category
    return ""


def slugify(text):
    """Convert text to slug format."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    return text[:30]  # Limit slug length


def extract_style_slug(product_url):
    """Extract style_slug from product URL.
    Works for both Buddha Pants (/products/) and Banana Republic (/browse/product.do?pid=).
    """
    if not product_url:
        return ""
    
    # Try Buddha Pants format (/products/...)
    if '/products/' in product_url:
        slug = product_url.split('/products/')[-1].split('?')[0].split('#')[0]
        return slug
    
    # Try Banana Republic format (extract pid from query string)
    try:
        parsed = urlparse(product_url)
        qs = parse_qs(parsed.query)
        pid = qs.get("pid", [""])[0].strip()
        if pid:
            return pid
    except Exception:
        pass
    
    # Fallback: use last meaningful path segment
    try:
        parsed = urlparse(product_url)
        path_parts = [p for p in parsed.path.split('/') if p]
        if path_parts:
            return path_parts[-1]
    except Exception:
        pass
    
    return ""


def infer_color_from_slug(style_slug):
    """Try to infer a color name from style_slug if it starts with a common color word."""
    if not style_slug:
        return None
    
    slug_lower = style_slug.lower()
    
    for color in COLOR_WORDS:
        # Check if slug starts with color word followed by hyphen
        if slug_lower.startswith(f"{color}-"):
            return color.capitalize()
    
    return None


APPAREL_CATEGORIES = {
    'sweaters', 'tops', 'dresses', 'pants', 'skirts', 'shorts', 'jumpsuits',
    'top', 'dress', 'short', 'skirt', 'jumpsuit'
}


def is_category_apparel(category):
    """Determine if category is apparel (True) or accessories (False)."""
    if not category:
        return False
    return category.lower() in APPAREL_CATEGORIES


def infer_is_apparel(category: str) -> bool:
    """Infer is_apparel from category. Used for all retailers."""
    return (category or "").lower() in APPAREL_CATEGORIES


def get_source_site(base_url):
    """Extract domain from base URL."""
    if not base_url:
        return ""
    parsed = urlparse(base_url)
    return parsed.netloc


def normalize_category(category):
    """Normalize category to fixed set of values."""
    if not category:
        return "unknown"
    cat_lower = category.lower()
    
    # Map raw categories to normalized ones
    category_map = {
        'sweaters': 'sweaters',
        'pants': 'pants',
        'jumpsuits': 'jumpsuit',
        'tops': 'top',
        'dresses': 'dress',
        'shorts': 'short',
        'skirts': 'skirt',
        'accessories': 'accessory',
    }
    
    return category_map.get(cat_lower, 'unknown')


def capitalize_color(color_name):
    """Capitalize color name nicely (e.g., 'black' -> 'Black')."""
    if not color_name:
        return ""
    # Handle multi-word colors (e.g., 'forest green' -> 'Forest Green')
    return ' '.join(word.capitalize() for word in color_name.split())


def extract_price_and_currency(price_text):
    """Extract price and currency from price text. Returns (price_raw, price_numeric, currency)."""
    if not price_text:
        return "", "", ""
    
    price_text = price_text.strip()
    currency_patterns = {"$": "USD", "USD": "USD", "£": "GBP", "€": "EUR", "CAD": "CAD", "AUD": "AUD"}
    
    currency = ""
    for symbol, code in currency_patterns.items():
        if symbol in price_text:
            currency = code
            break
    
    # Extract numeric price
    price_numeric = ""
    match = re.search(r'[\d.]+', price_text)
    if match:
        price_numeric = match.group()
    
    return price_text, price_numeric, currency


def clean_text(text):
    """Clean and normalize text."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def generate_ids(product_url, color_name=""):
    """Generate stable style_id and unique color_id from product URL and color name."""
    style_id = hashlib.md5(product_url.encode()).hexdigest()[:12]
    
    # Build color_id as style_id-slugified_color_name
    if color_name and color_name.lower() != "default":
        slugged = slugify(color_name)
        color_id = f"{style_id}-{slugged}"
    else:
        color_id = style_id
    
    return style_id, color_id


def normalize_image_url(url, base_url):
    """Normalize image URL to absolute form."""
    if not url:
        return ""
    url = str(url).strip()
    if url.startswith('//'):
        return 'https:' + url
    elif url.startswith('http'):
        return url
    else:
        return urljoin(base_url, url)


def extract_srcset_urls(srcset_str, limit=5):
    """Extract URLs from srcset attribute. Splits on commas, takes URL portion, limits to 5."""
    if not srcset_str:
        return []
    
    urls = []
    for entry in srcset_str.split(','):
        entry = entry.strip()
        if not entry:
            continue
        # Format is typically: "url 1x" or "url 100w" - split by whitespace and take first part
        url_part = entry.split()[0].strip()
        if url_part and url_part not in urls:  # Avoid duplicates
            urls.append(url_part)
            if len(urls) >= limit:
                break
    
    return urls


def is_product_image(url, style_slug):
    """Check if a URL is likely a product image (not logo, icon, etc)."""
    if not url:
        return False
    url_lower = url.lower()
    
    # Exclude generic site-wide icons and marketing assets first
    exclude_patterns = [
        '/assets/', '/cdn/shop/t/', '/cdn/shop/c/', 
        'logo', 'icon', 'badge', 'star', 'trustpilot', 'placeholder', 'avatar',
        'why-shop-icons', 'generic', 'default', 'button', 'arrow', 'chevron'
    ]
    for pattern in exclude_patterns:
        if pattern in url_lower:
            return False
    
    # For Buddha Pants: must contain /products/ in URL path
    if '/products/' in url_lower:
        return True
    
    # For style_slug matching (backup)
    if style_slug and style_slug in url_lower:
        return True
    
    return False


def extract_product_images_with_handle(soup, product_url, base_url, style_slug):
    """Extract product images using handle-based matching to avoid generic fallback images.
    Collects candidates from gallery selectors, filters generics, and prefers images matching the product handle.
    """
    # Extract product handle from URL
    handle = product_url.rstrip("/").split("/")[-1]
    handle_normalized = handle.replace("-", "").lower()
    
    # Collect candidate images from product gallery selectors
    candidates = []
    seen_urls = set()
    
    for selector in [".product__media img", ".product-gallery img", ".product-image img", "img"]:
        try:
            for img in soup.select(selector):
                src = img.get("src") or img.get("data-src") or ""
                if not src:
                    continue
                src = src.strip()
                
                # Normalize URL
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = "https://www.buddhapants.com" + src
                
                # Deduplicate
                if src in seen_urls:
                    continue
                seen_urls.add(src)
                candidates.append(src)
        except Exception:
            pass
    
    # Filter out marketing icons and generic assets
    exclude_patterns = [
        "why-shop-icons", "icon", "logo", "sprite", "/collections/", 
        "badge", "trustpilot", "placeholder", "avatar", "generic"
    ]
    filtered = [u for u in candidates if not any(pat in u.lower() for pat in exclude_patterns)]
    if not filtered:
        filtered = candidates
    
    # Prefer images whose filename contains the product handle
    preferred = [u for u in filtered if handle_normalized in u.replace("-", "").lower()]
    
    # Use preferred if available, otherwise use first filtered candidate
    if preferred:
        return preferred
    elif filtered:
        return [filtered[0]]
    elif candidates:
        return [candidates[0]]
    else:
        return []


def clean_gallery_images(gallery_urls, style_slug, limit=5):
    """Clean gallery images: filter product images only, deduplicate, limit to 5."""
    if not gallery_urls:
        return []
    
    cleaned = []
    seen = set()
    
    for url in gallery_urls:
        if url and url not in seen and is_product_image(url, style_slug):
            cleaned.append(url)
            seen.add(url)
            if len(cleaned) >= limit:
                break
    
    return cleaned


def extract_materials_snippet(materials_text):
    """Extract a short materials snippet with fiber + % or organic. Fallback to first 180 chars."""
    if not materials_text:
        return ""
    
    # Split into sentences/lines
    lines = re.split(r'[.\n]', materials_text)
    
    for line in lines:
        line = line.strip()
        if len(line) < 5:
            continue
        
        line_lower = line.lower()
        has_fiber = any(fiber in line_lower for fiber in FIBER_KEYWORDS)
        has_percent = '%' in line
        has_organic = 'organic' in line_lower
        
        # Found a line with fiber + (% or organic)
        if has_fiber and (has_percent or has_organic):
            # Trim to reasonable length
            return line[:300].strip()
    
    # Fallback: first 180-200 characters
    return materials_text[:180].strip()


def extract_focused_materials(soup, site_config=None):
    """Extract focused materials text: lines containing fiber words or %."""
    if site_config is None:
        site_config = SITE_CONFIG
    
    fiber_lines = []
    
    for selector in site_config["selectors"]["materials"].split(", "):
        try:
            for elem in soup.select(selector):
                text = clean_text(elem.get_text())
                if not text or len(text) < 10:
                    continue
                
                # Split into lines/sentences
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                
                for line in lines:
                    # Check if line contains fiber info
                    line_lower = line.lower()
                    has_fiber = any(fiber in line_lower for fiber in FIBER_KEYWORDS)
                    has_percent = '%' in line
                    
                    if (has_fiber or has_percent) and len(line) > 5:
                        fiber_lines.append(line)
        except Exception:
            pass
    
    # Return joined fiber lines if found
    if fiber_lines:
        result = " ".join(fiber_lines)
        return result[:2000] if result else ""
    
    # Fallback: get full description
    for selector in site_config["selectors"]["description"].split(", "):
        try:
            elem = soup.select_one(selector)
            if elem:
                text = clean_text(elem.get_text())
                if text:
                    return text[:2000]
        except Exception:
            pass
    
    # Final fallback: get any text with fiber info
    all_text = clean_text(soup.get_text())
    if all_text:
        return all_text[:2000]
    
    return ""


def extract_variants_from_json(soup, base_url, style_slug):
    """Extract color variants from Shopify JSON data with real color names."""
    variants = {}
    
    for script in soup.find_all('script', type='application/json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and 'variants' in data:
                for variant in data['variants']:
                    # Extract color name from variant title
                    title = variant.get('title', '').strip()
                    if not title:
                        continue
                    
                    # Parse variant title - typically "Color / Size" or just "Color"
                    parts = [p.strip() for p in title.split('/')]
                    color_name = parts[0] if parts else ''
                    
                    if not color_name or color_name.lower() == 'default':
                        continue
                    
                    # Store variant (avoid duplicates by color name)
                    if color_name not in variants:
                        variants[color_name] = {
                            'color_name': color_name,
                            'images': []
                        }
                    
                    # Extract image from variant
                    if 'featured_image' in variant and variant['featured_image']:
                        img_url = normalize_image_url(variant['featured_image'].get('src', ''), base_url)
                        if img_url and img_url not in variants[color_name]['images']:
                            variants[color_name]['images'].append(img_url)
        except Exception:
            pass
    
    return variants


# =============================================================================
# BANANA REPUBLIC HELPERS
# =============================================================================

def derive_banana_republic_style_id(product_url: str) -> str:
    """
    Derive a stable style_id from the Banana Republic product URL.
    Example URL:
      https://bananarepublic.gap.com/browse/product.do?pid=543262202&vid=1&pcid=5032&cid=5032
    We use the pid as the style_id. If pid is missing, fall back to the full URL.
    """
    try:
        parsed = urlparse(product_url)
        qs = parse_qs(parsed.query)
        pid = qs.get("pid", [""])[0].strip()
        if pid:
            return pid
        # fallback: use the path if pid is missing
        return parsed.path or product_url
    except Exception:
        return product_url


def slugify(text: str) -> str:
    """Convert text to a slug (lowercase, spaces to hyphens, strip special chars)."""
    if not text:
        return ""
    slug = text.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')


def looks_like_price(text: str) -> bool:
    """Check if text looks like a price string (contains $ or digit patterns with .)."""
    if not text:
        return False
    text = text.strip()
    # Check for dollar sign
    if "$" in text:
        return True
    # Check for digit patterns like "123.00"
    if re.search(r'\d+\.\d{2}', text):
        return True
    return False


def hex_to_color_name(hex_color: str) -> str:
    """Convert hex color to a human-readable color name.
    Uses webcolors to find the nearest CSS color name.
    """
    try:
        hex_color = hex_color.strip()
        if not hex_color.startswith('#'):
            hex_color = '#' + hex_color
        # Try exact match first
        try:
            return webcolors.hex_to_name(hex_color)
        except ValueError:
            # No exact match, find nearest color
            rgb = webcolors.hex_to_rgb(hex_color)
            nearest = webcolors.rgb_to_hex(webcolors.rgb_to_name(rgb))
            return webcolors.hex_to_name(nearest)
    except Exception as e:
        logger.debug(f"Could not convert hex {hex_color} to color name: {e}")
        return "Default"


def extract_dominant_color(image_url: str, use_fallback: bool = False) -> str:
    """Extract dominant color from image URL and return color name.
    Returns color name or "Default" if extraction fails or fallback disabled.
    """
    if not use_fallback or not image_url:
        return "Default"
    
    try:
        # Download image
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Extract dominant color
        img_file = BytesIO(response.content)
        color_thief = ColorThief(img_file)
        dominant_rgb = color_thief.get_color(quality=1)
        
        # Convert RGB to hex
        hex_color = '#{:02x}{:02x}{:02x}'.format(dominant_rgb[0], dominant_rgb[1], dominant_rgb[2])
        
        # Convert hex to color name
        color_name = hex_to_color_name(hex_color)
        
        logger.debug(f"Extracted color from image: {hex_color} -> {color_name}")
        return color_name
    except Exception as e:
        logger.debug(f"Could not extract color from image {image_url}: {e}")
        return "Default"


def extract_color_name_from_page(soup, image_url: str = "", use_hex_fallback: bool = False) -> str:
    """Extract the currently displayed/selected color name from BR product page.
    Validates against price strings and falls back to hex analysis if needed.
    Returns the color name or "Default" if not found.
    """
    candidate = ""
    
    # Try primary selectors with strict validation
    el = soup.select_one("[data-colorname]")
    if not el:
        el = soup.select_one(".colorName")
    
    if el:
        # Try to get color from attribute first
        candidate = el.get("data-colorname") or el.get_text(strip=True) or ""
        candidate = candidate.strip()
    
    # Reject if it looks like a price
    if candidate and looks_like_price(candidate):
        candidate = ""
    
    # Use candidate if valid, otherwise "Default"
    color_name = candidate or "Default"
    
    # Fallback: extract from image if enabled and we got "Default"
    if use_hex_fallback and color_name == "Default":
        color_name = extract_dominant_color(image_url, use_fallback=True)
    
    return color_name


def infer_category_from_title_and_url(product_title: str, product_url: str, config: dict) -> str:
    """
    Infer category from product title and URL for Banana Republic products.
    Looks for sweaters in URL/title first, then uses category_keywords from config.
    """
    title_lower = (product_title or "").lower()
    url_lower = (product_url or "").lower()
    
    # If the URL clearly indicates sweaters, use that first
    if "/women/sweaters" in url_lower or "cid=5032" in url_lower:
        return "sweaters"
    
    # Use category_keywords from config
    keywords = config.get("category_keywords", {})
    for category, words in keywords.items():
        for w in words:
            if w.lower() in title_lower:
                return category
    
    # Fallback to sweaters for BR integration
    return "sweaters"


def extract_banana_republic_data(html: str, product_url: str, config: dict) -> list:
    """Extract Banana Republic product data from rendered HTML DOM.
    Returns one record per color variant.
    """
    records = []
    
    # Handle both string and response objects
    if hasattr(html, 'text'):
        html = html.text
    
    soup = BeautifulSoup(html, 'lxml')
    
    try:
        # Extract product title from h1
        product_title = ""
        title_elem = soup.select_one("h1")
        if title_elem:
            product_title = clean_text(title_elem.get_text())
        
        # Extract price from span with price class or data attribute
        price_raw = ""
        currency = "USD"
        price_elem = soup.select_one("span[class*='price'], [data-price]")
        if price_elem:
            price_text = clean_text(price_elem.get_text())
            # Try to extract numeric value
            price_match = re.search(r'[\d,]+\.?\d*', price_text.replace('$', '').replace(',', ''))
            if price_match:
                price_raw = f"${price_match.group()}"
            else:
                price_raw = price_text
        
        # Extract materials from product information list
        materials = ""
        materials_parts = []
        info_list = soup.select("ul.product-information-item__list li span, ul.product-information-item__list li")
        for item in info_list:
            text = clean_text(item.get_text())
            if text and len(text) > 3:  # Skip very short items
                materials_parts.append(text)
        
        if materials_parts:
            materials = " ".join(materials_parts)
        else:
            # Fallback: look for any text with fiber keywords
            for tag in soup.find_all(['p', 'div', 'li']):
                text = clean_text(tag.get_text()).lower()
                if any(fiber in text for fiber in ['cotton', 'polyester', 'silk', 'wool', 'linen', 'fabric', 'material', '%']):
                    materials = clean_text(tag.get_text())
                    break
        
        # Extract gallery images using BR-specific selectors
        gallery_images = extract_br_gallery_images(soup, config["base_url"])
        image_url = gallery_images[0] if gallery_images else ""
        
        # Ensure gallery_image_urls is non-empty (fallback to image_url if needed)
        if not gallery_images and image_url:
            gallery_images = [image_url]
        
        gallery_urls_str = ";".join(gallery_images) if gallery_images else ""
        
        # Get stable style_id from product URL (pid parameter)
        style_id = derive_banana_republic_style_id(product_url)
        style_slug = extract_style_slug(product_url)
        
        # Generate materials snippet
        materials_snippet = extract_materials_snippet(materials)
        if not materials_snippet:
            materials_snippet = materials[:180] if materials else "No materials info found"
        
        # Determine category using BR-specific inference
        category = infer_category_from_title_and_url(product_title, product_url, config)
        normalized_category = normalize_category(category) if category else "sweaters"
        
        # Extract numeric price
        price_numeric = ""
        if price_raw:
            price_match = re.search(r'[\d,]+\.?\d*', price_raw.replace('$', '').replace(',', ''))
            if price_match:
                price_numeric = price_match.group()
        
        # Determine scrape status
        scrape_status = "ok"
        if not price_raw:
            scrape_status = "missing_price"
        elif not materials:
            scrape_status = "missing_materials"
        
        # Extract the displayed color name from DOM (with optional hex fallback)
        use_hex_fallback = config.get("use_hex_fallback", False)
        color_name = extract_color_name_from_page(soup, image_url, use_hex_fallback)
        color_id = slugify(color_name)
        
        # Build single record for this product with the extracted color
        record = {
            "schema_version": SCHEMA_VERSION,
            "style_id": style_id,
            "style_slug": style_slug,
            "color_id": color_id,
            "color_name": capitalize_color(color_name),
            "image_url": image_url,
            "gallery_image_urls": gallery_urls_str,
            "product_url": product_url,
            "brand_name": config["brand_name"],
            "source_site": get_source_site(config["base_url"]),
            "product_title": product_title,
            "category": normalized_category,
            "is_apparel": infer_is_apparel(normalized_category),
            "price_raw": price_raw,
            "price": price_numeric,
            "currency": currency,
            "materials_raw_or_page_text": materials,
            "materials_snippet": materials_snippet,
            "scrape_status": scrape_status,
        }
        
        # Add fabric-aware columns
        breakdown, tags = parse_fabric_breakdown(materials_snippet or materials or "")
        record["fabric_breakdown_pretty"] = breakdown
        record["fabric_tags"] = " + ".join(tags) if tags else None
        record["occasion_tag"] = classify_occasion(product_title or "", normalized_category or "")
        
        records.append(record)
        
    except Exception as e:
        logger.debug(f"Error extracting BR data: {e}")
    
    return records


def extract_br_gallery_images(soup, base_url):
    """Extract gallery images from Banana Republic's pdp-photo-single-column-image divs."""
    images = []
    
    try:
        for div in soup.select("div.pdp-photo-single-column-image"):
            # Try data-imageurl first
            img_url = div.get("data-imageurl")
            if not img_url:
                # Fall back to child img src
                img_elem = div.select_one("img")
                if img_elem:
                    img_url = img_elem.get("src")
            
            if img_url:
                # Normalize to full URL
                if img_url.startswith("/"):
                    img_url = base_url.rstrip("/") + img_url
                elif not img_url.startswith("http"):
                    img_url = urljoin(base_url, img_url)
                
                if img_url and img_url not in images:
                    images.append(img_url)
    except Exception as e:
        logger.warning(f"Error extracting BR gallery images: {e}")
    
    return images


def extract_gallery_images(soup, base_url, site_config=None, style_slug=""):
    """Extract all gallery/product images from the page, filtered for product images only."""
    if site_config is None:
        site_config = SITE_CONFIG
    
    images = []
    selector = site_config["selectors"].get("gallery_images", "")
    
    if selector:
        try:
            for img in soup.select(selector):
                src = img.get('src') or img.get('data-src') or ''
                srcset = img.get('srcset') or img.get('data-srcset') or ''
                
                # Handle srcset attribute (for sites like Banana Republic)
                if srcset and not src:
                    srcset_urls = extract_srcset_urls(srcset, limit=5)
                    for url in srcset_urls:
                        img_url = normalize_image_url(url, base_url)
                        if img_url and img_url not in images and is_product_image(img_url, style_slug):
                            images.append(img_url)
                elif src:
                    img_url = normalize_image_url(src, base_url)
                    if img_url and img_url not in images and is_product_image(img_url, style_slug):
                        images.append(img_url)
        except Exception:
            pass
    
    # Also look for images in product media containers
    for media in soup.select('[class*="product"] img, [class*="gallery"] img, [class*="media"] img'):
        src = media.get('src') or media.get('data-src') or ''
        if src:
            img_url = normalize_image_url(src, base_url)
            if img_url and img_url not in images and is_product_image(img_url, style_slug):
                images.append(img_url)
    
    return images


# =============================================================================
# SCRAPING FUNCTIONS
# =============================================================================

def get_product_links_from_collection(session, collection_url, robot_parser, site_config=None):
    """Extract all product links from a collection page."""
    if site_config is None:
        site_config = SITE_CONFIG
    
    product_urls = set()
    current_url = collection_url
    page_num = 1
    consecutive_empty = 0
    
    while current_url:
        logger.info(f"Collection page {page_num}: {current_url}")
        html = fetch_page(session, current_url, robot_parser)
        
        if not html:
            break
        
        soup = BeautifulSoup(html, 'lxml')
        before = len(product_urls)
        
        # Find product links
        for selector in site_config["selectors"]["product_link"].split(", "):
            try:
                for link in soup.select(selector):
                    href = link.get('href', '')
                    if href and ('/products/' in href or '/shop/p/' in href):
                        full_url = urljoin(site_config["base_url"], str(href))
                        product_urls.add(full_url)
            except Exception:
                pass
        
        new_count = len(product_urls) - before
        if new_count == 0:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
        else:
            consecutive_empty = 0
        
        # Find next page
        next_url = None
        for selector in site_config["selectors"]["pagination_next"].split(", "):
            try:
                link = soup.select_one(selector)
                if link and link.get('href'):
                    next_url = urljoin(site_config["base_url"], str(link['href']))
                    break
            except Exception:
                pass
        
        if not next_url and page_num < 20:
            if '?page=' in current_url:
                next_page = page_num + 1
                next_url = re.sub(r'\?page=\d+', f'?page={next_page}', current_url)
            elif page_num == 1:
                next_url = f"{collection_url}?page=2"
        
        if next_url and next_url != current_url:
            current_url = next_url
            page_num += 1
        else:
            current_url = None
        
        if page_num > 20:
            break
    
    logger.info(f"Found {len(product_urls)} unique products")
    return product_urls


def scrape_product_page(session, product_url, robot_parser, site_config=None):
    """Scrape a product page and extract all color variants with real names and clean gallery."""
    if site_config is None:
        site_config = SITE_CONFIG
    
    logger.info(f"Scraping: {product_url}")
    html = fetch_page(session, product_url, robot_parser)
    
    if not html:
        logger.error(f"Failed to fetch {product_url}")
        return []
    
    try:
        soup = BeautifulSoup(html, 'lxml')
    except Exception as e:
        logger.error(f"Failed to parse HTML for {product_url}: {e}")
        return []
    
    # Check if this is Banana Republic with special JSON parsing
    if site_config.get("parser_type") == "banana_republic":
        return scrape_banana_republic_product(html, soup, product_url, site_config)
    
    # Extract style_slug from URL
    style_slug = extract_style_slug(product_url)
    
    # Extract product-level data
    product_title = ""
    for selector in site_config["selectors"]["product_title"].split(", "):
        try:
            elem = soup.select_one(selector)
            if elem:
                product_title = clean_text(elem.get_text())
                if product_title:
                    break
        except Exception:
            pass
    
    # Extract price
    price_raw = ""
    price_numeric = ""
    currency = ""
    
    price_meta = soup.find('meta', {'property': 'product:price:amount'})
    currency_meta = soup.find('meta', {'property': 'product:price:currency'})
    
    if price_meta and price_meta.get('content'):
        price_value = price_meta.get('content')
        currency_value = currency_meta.get('content') if currency_meta else "USD"
        price_numeric = price_value
        price_raw = f"${price_value}" if currency_value == "USD" else f"{price_value} {currency_value}"
        currency = str(currency_value) if currency_value else "USD"
    else:
        for selector in site_config["selectors"]["price"].split(", "):
            try:
                elem = soup.select_one(selector)
                if elem:
                    price_raw = clean_text(elem.get_text())
                    if price_raw:
                        break
            except Exception:
                pass
        price_raw, price_numeric, currency = extract_price_and_currency(price_raw)
    
    # Extract main image - try selectors first (more reliable for product-specific images)
    main_image = ""
    
    for selector in site_config["selectors"]["main_image"].split(", "):
        try:
            elem = soup.select_one(selector)
            if elem:
                src = elem.get('src') or elem.get('data-src') or ''
                srcset = elem.get('srcset') or ''
                if srcset and not src:
                    # Extract from srcset if no direct src
                    srcset_urls = extract_srcset_urls(srcset, limit=1)
                    if srcset_urls:
                        candidate = normalize_image_url(srcset_urls[0], site_config["base_url"])
                        if is_product_image(candidate, style_slug):
                            main_image = candidate
                            break
                elif src:
                    candidate = normalize_image_url(src, site_config["base_url"])
                    if is_product_image(candidate, style_slug):
                        main_image = candidate
                        break
        except Exception:
            pass
    
    # Fallback to og:image only if selector extraction found nothing
    if not main_image:
        og_image = soup.find('meta', {'property': 'og:image'})
        if og_image and og_image.get('content'):
            main_image = normalize_image_url(og_image.get('content'), site_config["base_url"])
    
    # Extract focused materials text
    materials_text = extract_focused_materials(soup, site_config)
    
    # Generate materials snippet
    materials_snippet = extract_materials_snippet(materials_text)
    if not materials_snippet:
        materials_snippet = materials_text[:180] if materials_text else "No materials info found"
    
    category = detect_category(product_url, product_title)
    
    # Extract variants from JSON (with featured_image from Shopify JSON)
    variants_dict = extract_variants_from_json(soup, site_config["base_url"], style_slug)
    
    if not variants_dict:
        # No variants found in JSON - try to infer color from style_slug
        inferred_color = infer_color_from_slug(style_slug)
        if inferred_color:
            color_name = inferred_color
        else:
            # Fallback to product title
            color_name = product_title if product_title else "default"
        # For products with no JSON variants, use main_image as fallback
        fallback_images = [main_image] if main_image else []
        variants_dict = {color_name: {"color_name": color_name, "images": fallback_images}}
    
    # Generate style_id
    style_id, _ = generate_ids(product_url)
    
    # Determine scrape status
    scrape_status = "ok"
    if not price_raw:
        scrape_status = "missing_price"
    elif not materials_text:
        scrape_status = "missing_materials"
    
    # Normalize category and capitalize color names
    normalized_category = normalize_category(category)
    
    # Create records for each color variant
    records = []
    seen_colors = set()
    
    for color_name, variant_data in variants_dict.items():
        if color_name in seen_colors:
            continue
        seen_colors.add(color_name)
        
        # Generate color_id with slug format
        _, color_id = generate_ids(product_url, color_name)
        
        # Use variant images from Shopify JSON (featured_image for each variant)
        variant_images = variant_data.get('images', [])
        if not variant_images and main_image:
            variant_images = [main_image]
        
        # Clean gallery images: filter to product images, deduplicate, limit to 5
        clean_gallery = clean_gallery_images(variant_images, style_slug, limit=5)
        if not clean_gallery and main_image:
            clean_gallery = [main_image]
        
        image_url = clean_gallery[0] if clean_gallery else main_image
        gallery_urls = "|".join(clean_gallery) if clean_gallery else ""
        
        record = {
            "schema_version": SCHEMA_VERSION,
            "style_id": style_id,
            "style_slug": style_slug,
            "color_id": color_id,
            "color_name": capitalize_color(color_name),
            "image_url": image_url,
            "gallery_image_urls": gallery_urls,
            "product_url": product_url,
            "brand_name": site_config["brand_name"],
            "source_site": get_source_site(site_config["base_url"]),
            "product_title": product_title,
            "category": normalized_category,
            "is_apparel": is_category_apparel(normalized_category),
            "price_raw": price_raw,
            "price": price_numeric,
            "currency": currency,
            "materials_raw_or_page_text": materials_text,
            "materials_snippet": materials_snippet,
            "scrape_status": scrape_status,
        }
        
        # Add fabric-aware columns
        breakdown, tags = parse_fabric_breakdown(materials_snippet or materials_text or "")
        record["fabric_breakdown_pretty"] = breakdown
        record["fabric_tags"] = " + ".join(tags) if tags else None
        record["occasion_tag"] = classify_occasion(product_title or "", normalized_category or "")
        
        records.append(record)
    
    return records


def scrape_banana_republic_product(html, soup, product_url, site_config):
    """Scrape Banana Republic product from rendered HTML."""
    # extract_banana_republic_data now returns complete records
    records = extract_banana_republic_data(html, product_url, site_config)
    return records


def save_to_csv(records, filename):
    """Save records to CSV file with deduplication and sorting."""
    if not records:
        logger.warning("No records to save!")
        return
    
    # Deduplicate by style_id + color_name (keep first occurrence)
    seen = set()
    deduplicated = []
    duplicates = 0
    
    for record in records:
        dedup_key = (record['style_id'], record['color_name'])
        if dedup_key not in seen:
            seen.add(dedup_key)
            deduplicated.append(record)
        else:
            duplicates += 1
    
    if duplicates > 0:
        logger.info(f"Removed {duplicates} duplicate records (by style_id + color_name)")
    
    # Sort by brand_name, style_slug, color_name for consistency
    sorted_records = sorted(deduplicated, key=lambda r: (r['brand_name'], r['style_slug'], r['color_name']))
    
    logger.info(f"Saving {len(sorted_records)} unique records to {filename}")
    
    # Debug: check if first record has fabric columns
    if sorted_records:
        first_rec = sorted_records[0]
        has_breakdown = 'fabric_breakdown_pretty' in first_rec
        has_tags = 'fabric_tags' in first_rec
        has_occasion = 'occasion_tag' in first_rec
        logger.debug(f"First record has fabric_breakdown_pretty: {has_breakdown}")
        logger.debug(f"First record has fabric_tags: {has_tags}")
        logger.debug(f"First record has occasion_tag: {has_occasion}")
        logger.debug(f"Sample fabric_breakdown: {first_rec.get('fabric_breakdown_pretty', 'N/A')}")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(sorted_records)
    
    logger.info(f"Successfully saved to {filename}")


# =============================================================================
# MAIN SCRAPER
# =============================================================================

def run_banana_republic_scraper(config):
    """Scrape Banana Republic using Playwright for client-side rendered content."""
    # Replit environment guard - browser dependencies not available
    if os.environ.get("REPL_ID"):
        print("\n" + "=" * 70)
        print("⚠️  Banana Republic Scraper - Replit Limitation")
        print("=" * 70)
        print("\nHeadless browser scraping for Banana Republic is not supported in this")
        print("Replit environment because the required system libraries for Playwright")
        print("Firefox/Chromium are not available in the Nix sandbox.")
        print("\n✓ You can still run: python main.py buddhapants (works in Replit)")
        print("✓ To scrape Banana Republic, run this project locally on your own machine")
        print("\nSee LOCAL_BANANA_REPUBLIC_SETUP.md for local setup instructions.")
        print("=" * 70 + "\n")
        return
    
    logger.info("=" * 70)
    logger.info(f"Starting Loomi Scraper for {config['name']} (Playwright mode)")
    logger.info("=" * 70)
    
    all_records = []
    failed_products = 0
    
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        
        # Collect product URLs from all collection pages
        all_product_urls = set()
        
        for collection_url in config["collection_urls"]:
            logger.info(f"\nProcessing collection: {collection_url}")
            
            try:
                page.goto(collection_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)  # 3,000 ms = 3 seconds
                logger.info(f"Page loaded: {collection_url}")
                
                # Scroll to bottom to load all products
                scroll_count = 0
                max_scrolls = 10
                last_count = 0
                
                # Try to find product links with different selectors
                selectors_to_try = [
                    'a[href*="product.do?pid="]',
                    'a[href*="/browse/product.do"]',
                    'a[href*="pid="]',
                ]
                
                working_selector = None
                for test_selector in selectors_to_try:
                    test_links = page.query_selector_all(test_selector)
                    logger.info(f"DEBUG: Testing selector '{test_selector}' -> found {len(test_links)} links")
                    if len(test_links) > 0:
                        working_selector = test_selector
                        logger.info(f"DEBUG: Using working selector: {working_selector}")
                        break
                
                # If no selector worked, try using the config selector
                if not working_selector:
                    product_link_selector = config["selectors"].get("product_link", "a[href*='product.do?pid=']")
                    working_selector = product_link_selector.split(", ")[0]  # Use first selector from config
                    logger.info(f"DEBUG: Using config selector: {working_selector}")
                
                while scroll_count < max_scrolls:
                    page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                    time.sleep(1)  # Wait for lazy load
                    
                    # Count visible product links
                    current_links = page.query_selector_all(working_selector)
                    logger.info(f"Scroll {scroll_count}, links so far: {len(current_links)}")
                    
                    if len(current_links) == last_count:
                        break  # No new products loaded
                    
                    last_count = len(current_links)
                    scroll_count += 1
                
                logger.info(f"Found {len(current_links)} product links after scrolling")
                
                # Extract all product URLs from the DOM
                for link in current_links:
                    href = link.get_attribute('href')
                    if href and '/browse/product.do?pid=' in href:
                        full_url = urljoin(config["base_url"], href)
                        all_product_urls.add(full_url)
                
                logger.info(f"Found {len(all_product_urls)} unique products so far")
                
                # Limit to first 30 products for testing
                if len(all_product_urls) >= 30:
                    logger.info("Reached 30 products limit for this run")
                    break
                    
            except Exception as e:
                logger.error(f"Error processing collection {collection_url}: {e}")
        
        logger.info(f"\nTotal unique products to scrape: {len(all_product_urls)}")
        
        # Scrape each product page
        for i, product_url in enumerate(sorted(all_product_urls), 1):
            try:
                logger.info(f"\n[{i}/{len(all_product_urls)}] Scraping: {product_url}")
                page.goto(product_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)  # Give lazy loaders time to render
                
                # Get the full HTML after rendering
                html = page.content()
                soup = BeautifulSoup(html, 'lxml')
                
                # Use existing Banana Republic parser
                records = scrape_banana_republic_product(html, soup, product_url, config)
                
                if records:
                    all_records.extend(records)
                    for r in records:
                        logger.info(f"  {r['color_name']:25} | {r['product_title'][:30]} | {r['scrape_status']}")
                else:
                    failed_products += 1
                    logger.warning(f"No records extracted from {product_url}")
                
                # Random delay between requests (politeness)
                time.sleep(random.uniform(1.0, 2.0))
                
            except Exception as e:
                failed_products += 1
                logger.error(f"Error scraping {product_url}: {e}")
        
        context.close()
        browser.close()
    
    # Save CSV
    logger.info("\n" + "=" * 70)
    save_to_csv(all_records, config["output_file"])
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("SCRAPING COMPLETE")
    logger.info(f"Total color variants scraped: {len(all_records)}")
    logger.info(f"Failed products: {failed_products}")
    logger.info(f"Unique products: {len(all_product_urls)}")
    logger.info(f"Output file: {config['output_file']}")
    logger.info(f"Schema version: {SCHEMA_VERSION}")
    logger.info("=" * 70)
    
    if all_records:
        categories = {}
        statuses = {}
        for r in all_records:
            cat = r['category'] or 'unknown'
            categories[cat] = categories.get(cat, 0) + 1
            status = r['scrape_status']
            statuses[status] = statuses.get(status, 0) + 1
        logger.info("\nCategory breakdown:")
        for cat in sorted(categories.keys()):
            logger.info(f"  {cat}: {categories[cat]}")
        logger.info("\nScrape status breakdown:")
        for status in sorted(statuses.keys()):
            logger.info(f"  {status}: {statuses[status]}")


def run_scraper(config):
    """Main scraper function. Args: config - site configuration dict"""
    # Branch on parser type for BR vs Buddha Pants
    if config.get("parser_type") == "banana_republic":
        return run_banana_republic_scraper(config)
    
    logger.info("=" * 70)
    logger.info(f"Starting Loomi Scraper for {config['name']}")
    logger.info("=" * 70)
    
    session = get_session()
    robot_parser = check_robots_txt(config["base_url"])
    
    # Collect product URLs
    all_product_urls = set()
    for collection_url in config["collection_urls"]:
        logger.info(f"\nProcessing collection: {collection_url}")
        urls = get_product_links_from_collection(session, collection_url, robot_parser, config)
        all_product_urls.update(urls)
    
    logger.info(f"\nTotal unique products: {len(all_product_urls)}")
    
    # Scrape each product
    all_records = []
    failed_products = 0
    for i, product_url in enumerate(sorted(all_product_urls), 1):
        logger.info(f"\n[{i}/{len(all_product_urls)}]")
        records = scrape_product_page(session, product_url, robot_parser, config)
        if records:
            all_records.extend(records)
            for r in records:
                logger.info(f"  {r['color_name']:25} | {r['product_title'][:30]} | {r['scrape_status']}")
        else:
            failed_products += 1
            logger.warning(f"No records extracted from {product_url}")
    
    # Save CSV
    logger.info("\n" + "=" * 70)
    save_to_csv(all_records, config["output_file"])
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("SCRAPING COMPLETE")
    logger.info(f"Total color variants scraped: {len(all_records)}")
    logger.info(f"Failed products: {failed_products}")
    logger.info(f"Unique products: {len(all_product_urls)}")
    logger.info(f"Output file: {config['output_file']}")
    logger.info(f"Schema version: {SCHEMA_VERSION}")
    logger.info("=" * 70)
    
    if all_records:
        categories = {}
        statuses = {}
        for r in all_records:
            cat = r['category'] or 'unknown'
            categories[cat] = categories.get(cat, 0) + 1
            status = r['scrape_status']
            statuses[status] = statuses.get(status, 0) + 1
        logger.info("\nCategory breakdown:")
        for cat in sorted(categories.keys()):
            logger.info(f"  {cat}: {categories[cat]}")
        logger.info("\nScrape status breakdown:")
        for status in sorted(statuses.keys()):
            logger.info(f"  {status}: {statuses[status]}")


def main():
    """Main entry point for the scraper."""
    site_key = "buddhapants"
    if len(sys.argv) > 1:
        site_key = sys.argv[1].lower()

    if site_key not in SITE_CONFIGS:
        print(f"Unknown site '{site_key}'. Valid options: {', '.join(SITE_CONFIGS.keys())}")
        return

    config = SITE_CONFIGS[site_key]
    print(f"\nLoomi Scraper – running for: {config['name']}\n")
    run_scraper(config)


if __name__ == "__main__":
    main()
