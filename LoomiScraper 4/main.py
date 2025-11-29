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
from io import BytesIO
from fabric_parser import parse_fabric_breakdown
from scraper_engine import ScrapeManager, EnvironmentProbe
from site_profiles import get_site_profile, list_sites, SITE_PROFILES

# =============================================================================
# CONFIGURATION - Modify these settings for different sites
# =============================================================================

SCHEMA_VERSION = 3  # Added color analysis: hex codes, normalized names, patterns

# Enhanced CSV Schema with Loomi-required columns
CSV_FIELDNAMES = [
    "schema_version",
    "style_id",
    "style_slug",
    "color_id",
    "color_name",
    "color_name_normalized",
    "dominant_hex",
    "pattern_type",
    "color_family",
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
    # Loomi integration columns
    "audience",
    "subcategory",
    "sizes_available",
    "waist_sizes",
    "length",
    "natural_fiber_percent",
    "synthetic_fiber_percent",
    "is_100pct_natural",
    "is_loomi_approved",
    "fiber_families",
    "synthetic_fibers_present",
    "care_level",
    "description",
    "fabric_breakdown_pretty",
    "fabric_tags",
    "occasion_tag",
    # Extended product details
    "sku",
    "color_code",
    "fit_type",
    "care_instructions",
    "product_weight",
    "country_of_origin",
    "sustainability_tags",
    "season",
    "collection_name"
]

SITE_CONFIGS = {
    "buddhapants": {
        "name": "Buddha Pants",
        "base_url": "https://www.buddhapants.com",
        "collection_urls": [
            "https://www.buddhapants.com/collections/pants",
            "https://www.buddhapants.com/collections/tops",
            "https://www.buddhapants.com/collections/jumpsuits"
        ],
        "output_file": "buddhapants_raw.csv",
        "parser_type": "shopify",
        "selectors": {
            "product_links": "a.product-item__title",
            "product_title": "h1.product__title",
            "price": "span.price-item--regular",
            "materials": "div.product__description",
            "gallery_images": "div.product__media img"
        }
    },
    "bananarepublic": {
        "name": "Banana Republic",
        "base_url": "https://bananarepublic.gap.com",
        "collection_urls": [
            # Women — complete catalog + major garment types
              "https://bananarepublic.gap.com/browse/women?cid=5002",
              "https://bananarepublic.gap.com/browse/women/dresses-and-jumpsuits?cid=69883",
              "https://bananarepublic.gap.com/browse/women/pants?cid=67595",
              "https://bananarepublic.gap.com/browse/women/sweaters?cid=5032",
              "https://bananarepublic.gap.com/browse/women/workwear?cid=1178917",
              "https://bananarepublic.gap.com/browse/women/accessories?cid=1134528",

              # Men — complete catalog (includes sweaters, pants, etc.)
              "https://bananarepublic.gap.com/browse/men?cid=5343",

              # Optional catch-all / sale / clearance / mixed
              "https://bananarepublic.gap.com/browse/sale?cid=1014329",
              "https://bananarepublic.gap.com/"
        ],
        "output_file": "bananarepublic_raw.csv",
        "parser_type": "banana_republic",
        "selectors": {
            "product_links": "a.product-card__link, a[href*='/browse/product.do?pid=']",
            "product_title": "h1.product-name",
            "price": "span.product-price__highlight",
            "materials": "div.product-details__description",
            "gallery_images": "div.pdp-photo-single-column-image img"
        }
    }
}

SITE_CONFIG = SITE_CONFIGS["buddhapants"]

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# =============================================================================
# HELPERS
# =============================================================================

def get_session():
    """Create a requests session with headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    return session

def check_robots_txt(base_url):
    """Parse robots.txt for the site."""
    rp = RobotFileParser()
    rp.set_url(urljoin(base_url, "/robots.txt"))
    try:
        rp.read()
    except Exception:
        pass
    return rp

def can_fetch(robot_parser, url, user_agent="*"):
    """Check if URL can be fetched according to robots.txt."""
    try:
        return robot_parser.can_fetch(user_agent, url)
    except Exception:
        return True

def normalize_image_url(url, base_url):
    """Normalize image URL to absolute and clean format."""
    if not url:
        return ""
    url = url.split('?')[0]
    if url.startswith('//'):
        url = 'https:' + url
    elif not url.startswith('http'):
        url = urljoin(base_url, url)
    return url

def extract_srcset_urls(srcset, limit=5):
    """Extract URLs from srcset attribute, return up to 'limit' URLs."""
    urls = []
    for part in srcset.split(','):
        url = part.strip().split()[0]
        if url:
            urls.append(url)
        if len(urls) >= limit:
            break
    return urls

def is_product_image(img_url, style_slug=""):
    """Filter out non-product images (logos, icons, swatches)."""
    lower_url = img_url.lower()
    exclude_patterns = ['logo', 'icon', 'swatch', 'cart', 'payment', 'badge', 'social']
    if any(pattern in lower_url for pattern in exclude_patterns):
        return False
    if style_slug and style_slug.lower() not in lower_url:
        return False
    return True

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

    for media in soup.select('[class*="product"] img, [class*="gallery"] img, [class*="media"] img'):
        src = media.get('src') or media.get('data-src') or ''
        if src:
            img_url = normalize_image_url(src, base_url)
            if img_url and img_url not in images and is_product_image(img_url, style_slug):
                images.append(img_url)

    return images[:10]

def infer_audience(title, category):
    """Infer audience from product title and category."""
    title_lower = (title or "").lower()
    category_lower = (category or "").lower()
    text = f"{title_lower} {category_lower}"

    if any(kw in text for kw in ["men's", "men", "male"]):
        return "Men"
    if any(kw in text for kw in ["women's", "women", "female", "ladies"]):
        return "Women"
    if any(kw in text for kw in ["kids", "children", "child"]):
        return "Kids"
    if any(kw in text for kw in ["girls", "girl's"]):
        return "Girls"
    if any(kw in text for kw in ["boys", "boy's"]):
        return "Boys"
    if any(kw in text for kw in ["baby", "infant", "toddler"]):
        return "Baby"
    if any(kw in text for kw in ["unisex", "gender neutral"]):
        return "Unisex"

    return "Women"  # Default

def map_subcategory(category, title):
    """Map category and title to Loomi subcategory."""
    category_lower = (category or "").lower()
    title_lower = (title or "").lower()
    text = f"{category_lower} {title_lower}"

    # Non-apparel first
    if any(kw in text for kw in ["journal", "notebook", "notes", "sticker", "accessory", "scarf", "hat", "bag", "belt", "purse", "fanny", "clutch", "lanyard"]):
        return "Accessories"
    
    # Apparel
    if any(kw in text for kw in ["pant", "trouser", "jean", "short", "legging", "jogger", "harem"]):
        return "Bottoms"
    if any(kw in text for kw in ["top", "shirt", "blouse", "tee", "tank", "tunic"]):
        return "Tops"
    if any(kw in text for kw in ["dress", "gown", "frock"]):
        return "Dresses"
    if any(kw in text for kw in ["sweater", "cardigan", "pullover", "knit"]):
        return "Sweaters"
    if any(kw in text for kw in ["jacket", "coat", "blazer", "parka", "vest"]):
        return "Outerwear"
    if any(kw in text for kw in ["jumpsuit", "romper", "overall"]):
        return "Outerwear"
    if any(kw in text for kw in ["pajama", "sleepwear", "nightgown", "loungewear"]):
        return "Sleepwear"
    if any(kw in text for kw in ["hoodie"]):
        return "Tops"

    return "Tops"  # Default

def extract_sizes(soup, html_text):
    """Extract available sizes from product page."""
    sizes = []

    # Look for size selectors
    for select in soup.find_all(['select', 'div'], class_=lambda x: x and 'size' in x.lower()):
        for option in select.find_all(['option', 'button', 'span']):
            size_text = option.get_text(strip=True)
            if size_text and len(size_text) <= 10:
                sizes.append(size_text)

    # Look for size mentions in text
    size_pattern = r'\b(XXS|XS|S|M|L|XL|XXL|XXXL|\d+)\b'
    found_sizes = re.findall(size_pattern, html_text, re.IGNORECASE)
    sizes.extend(found_sizes)

    # Normalize and deduplicate
    valid_sizes = set()
    standard_sizes = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL']
    numeric_sizes = [str(i) for i in range(0, 20, 2)]  # 0, 2, 4, 6, 8, 10, 12, 14, 16, 18
    
    for s in sizes:
        s_upper = s.upper().strip()
        # Keep standard letter sizes
        if s_upper in standard_sizes:
            valid_sizes.add(s_upper)
        # Keep numeric sizes (even numbers 0-18)
        elif s in numeric_sizes:
            valid_sizes.add(s)
        # Skip noise like single digits or 'SIZE'
        elif s_upper in ['SIZE', 'SIZES', 'ONE']:
            continue
    
    # Sort: letter sizes first, then numeric
    size_order = {s: i for i, s in enumerate(['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL'])}
    sorted_sizes = sorted(valid_sizes, key=lambda x: (size_order.get(x, 100), int(x) if x.isdigit() else 0))

    return "|".join(sorted_sizes) if sorted_sizes else ""

def extract_waist_sizes(sizes_available, subcategory):
    """Extract numeric waist sizes if this is pants/bottoms."""
    if subcategory != "Bottoms":
        return ""

    waist_sizes = []
    for size in (sizes_available or "").split("|"):
        if size.isdigit() and 24 <= int(size) <= 50:
            waist_sizes.append(size)

    return "|".join(waist_sizes) if waist_sizes else ""

def extract_length(title, html_text):
    """Extract length options (Petite, Standard, Tall)."""
    text = f"{title} {html_text}".lower()

    if "petite" in text:
        return "Petite"
    if "tall" in text:
        return "Tall"

    return "Standard"

def analyze_fibers(fabric_breakdown):
    """Analyze fiber composition and return natural/synthetic percentages."""
    if not fabric_breakdown:
        return {
            "natural_fiber_percent": 0,
            "synthetic_fiber_percent": 0,
            "is_100pct_natural": False,
            "is_loomi_approved": False,
            "fiber_families": "",
            "synthetic_fibers_present": ""
        }

    natural_fibers = {"cotton", "linen", "wool", "silk", "hemp", "flax", "jute"}
    synthetic_fibers = {"polyester", "nylon", "acrylic", "rayon", "spandex", "elastane"}
    
    text_lower = fabric_breakdown.lower()
    total_percent = 0
    natural_total = 0
    found_fibers = []
    synthetic_found = []
    
    for fiber in natural_fibers:
        if fiber in text_lower:
            found_fibers.append(fiber)
            match = re.search(rf'{fiber}\s*(?:%|percent)?\s*(\d+)', text_lower)
            if match:
                natural_total += int(match.group(1))
            total_percent += 10
    
    for fiber in synthetic_fibers:
        if fiber in text_lower:
            synthetic_found.append(fiber)
            total_percent += 10
    
    return {
        "natural_fiber_percent": min(natural_total, 100),
        "synthetic_fiber_percent": min(100 - natural_total, 100),
        "is_100pct_natural": natural_total >= 100,
        "is_loomi_approved": natural_total >= 90,
        "fiber_families": "|".join(found_fibers),
        "synthetic_fibers_present": "|".join(synthetic_found)
    }


def extract_sku(soup, html_text, product_url):
    """Extract SKU/product code from page."""
    # Try common SKU patterns
    sku_patterns = [
        r'sku["\s:]+([A-Z0-9-]+)',
        r'product[_\s]?code["\s:]+([A-Z0-9-]+)',
        r'style["\s:]+([A-Z0-9-]+)',
        r'pid=([A-Z0-9-]+)',
        r'productId["\s:]+([A-Z0-9-]+)'
    ]
    
    for pattern in sku_patterns:
        match = re.search(pattern, html_text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    # Try structured data
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if data.get('sku'):
                return data['sku']
        except:
            pass
    
    return ""

def extract_fit_type(title, description, category):
    """Extract fit type from product information."""
    text = f"{title} {description} {category}".lower()
    
    fit_keywords = {
        "Slim": ["slim fit", "slim-fit", "fitted"],
        "Regular": ["regular fit", "classic fit", "standard fit"],
        "Relaxed": ["relaxed fit", "loose fit", "easy fit"],
        "Oversized": ["oversized", "loose", "boyfriend"],
        "Tailored": ["tailored", "structured"],
        "Athletic": ["athletic fit", "performance fit"]
    }
    
    for fit_type, keywords in fit_keywords.items():
        if any(kw in text for kw in keywords):
            return fit_type
    
    return "Regular"

def extract_care_instructions(soup, materials_text):
    """Extract detailed care instructions."""
    care_text = ""
    
    # Look for care instruction sections
    for elem in soup.find_all(['div', 'p', 'span'], class_=lambda x: x and 'care' in x.lower()):
        care_text += elem.get_text(separator=' ', strip=True) + " "
    
    # Common care patterns in materials text
    if materials_text:
        care_match = re.search(
            r'(machine wash|hand wash|dry clean|iron|tumble dry|do not bleach)[^.]*\.',
            materials_text.lower()
        )
        if care_match:
            care_text += care_match.group(0)
    
    return care_text.strip()[:500]

def extract_sustainability_info(soup, html_text):
    """Extract sustainability certifications and claims."""
    text = f"{soup.get_text()} {html_text}".lower()
    
    sustainability_markers = [
        "organic",
        "gots certified",
        "fair trade",
        "sustainable",
        "eco-friendly",
        "recycled",
        "carbon neutral",
        "b corp",
        "oeko-tex",
        "bluesign"
    ]
    
    found = [marker for marker in sustainability_markers if marker in text]
    return "|".join(found) if found else ""

def extract_country_of_origin(soup, html_text):
    """Extract manufacturing country."""
    patterns = [
        r'made in ([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)',
        r'manufactured in ([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)',
        r'country of origin[:\s]+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE)
        if match:
            return match.group(1).title()
    
    return ""

def extract_season(title, category, html_text):
    """Infer season/collection from product details."""
    text = f"{title} {category} {html_text}".lower()
    
    if any(kw in text for kw in ["spring", "summer", "ss"]):
        return "Spring/Summer"
    if any(kw in text for kw in ["fall", "autumn", "winter", "fw"]):
        return "Fall/Winter"
    if "resort" in text:
        return "Resort"
    if "holiday" in text:
        return "Holiday"
    
    return "All Season"


def determine_care_level(fiber_families):
    """Determine care level based on fiber types."""
    fibers = (fiber_families or "").lower()

    if any(f in fibers for f in ["cashmere", "silk"]):
        return "Delicate"
    if any(f in fibers for f in ["wool", "alpaca", "mohair"]):
        return "Gentle"

    return "Easy Care"

def generate_fabric_tags(fiber_analysis, fabric_breakdown_pretty):
    """Generate descriptive fabric tags."""
    if not fabric_breakdown_pretty:
        return ""

    is_100pct = fiber_analysis.get("is_100pct_natural", False)
    fibers = fiber_analysis.get("fiber_families", "").split("|")

    if not fibers or not fibers[0]:
        return ""

    primary_fiber = fibers[0].upper()

    if is_100pct and len(fibers) == 1:
        if "cotton" in fibers[0]:
            return "100% COTTON"
        elif "linen" in fibers[0]:
            return "100% LINEN"
        elif "wool" in fibers[0]:
            return "PREMIUM WOOL"
        elif "silk" in fibers[0]:
            return "PURE SILK"
        else:
            return f"100% {primary_fiber}"

    if len(fibers) >= 2:
        return f"{fibers[0].upper()} + {fibers[1].upper()} BLEND"

    return primary_fiber

def determine_occasion(title, category, subcategory):
    """Determine occasion tag based on product attributes."""
    text = f"{title} {category} {subcategory}".lower()

    # Non-apparel items
    if any(kw in text for kw in ["journal", "notebook", "notes", "sticker"]):
        return "Work & Meetings"
    if any(kw in text for kw in ["bag", "purse", "fanny", "clutch"]):
        return "Off-Duty & Travel"
    
    # Apparel
    if any(kw in text for kw in ["yoga", "lounge", "jogger", "casual", "relax", "comfort", "harem"]):
        return "Off-Duty & Travel"
    if any(kw in text for kw in ["formal", "office", "professional", "work", "business", "blazer"]):
        return "Work & Meetings"
    if any(kw in text for kw in ["party", "evening", "gown", "cocktail", "special", "elegant"]):
        return "Parties & Events"
    if any(kw in text for kw in ["beach", "vacation", "resort", "summer", "swim", "romper"]):
        return "Vacation & Leisure"
    if any(kw in text for kw in ["active", "sport", "athletic", "gym", "workout", "hoodie"]):
        return "Active & Fitness"

    return "Off-Duty & Travel"  # Default

def generate_description(title, materials_snippet, category):
    """Generate a 2-3 sentence product description."""
    if not title:
        return ""

    desc = f"{title}."

    if materials_snippet and len(materials_snippet) > 20:
        # Extract first meaningful sentence
        sentences = materials_snippet.split('.')
        if sentences:
            desc += f" {sentences[0].strip()}."

    if category:
        desc += f" Perfect addition to your {category.lower()} collection."

    return desc[:500]  # Limit length

def extract_dominant_color_hex(image_url, max_retries=2):
    """Extract dominant color hex code from image URL with retry logic."""
    if not image_url:
        return ""

    for attempt in range(max_retries):
        try:
            response = requests.get(
                image_url, 
                timeout=15, 
                stream=True,
                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'}
            )
            response.raise_for_status()

            # Validate we got image data
            content_type = response.headers.get('content-type', '')
            if 'image' not in content_type.lower():
                logger.debug(f"URL didn't return image: {content_type}")
                return ""

            # Use ColorThief to get dominant color
            image_data = BytesIO(response.content)
            color_thief = ColorThief(image_data)
            dominant_color = color_thief.get_color(quality=1)

            # Convert RGB to hex
            hex_code = '#{:02x}{:02x}{:02x}'.format(*dominant_color)
            return hex_code.upper()

        except requests.RequestException as e:
            if attempt < max_retries - 1:
                logger.debug(f"Retry {attempt + 1}/{max_retries} for color extraction: {e}")
                time.sleep(1)
            else:
                logger.debug(f"Failed to extract color after {max_retries} attempts: {e}")
        except Exception as e:
            logger.debug(f"Color extraction error on {image_url}: {e}")
            break
    
    return ""

def normalize_color_name(color_name, title="", description=""):
    """Normalize color name to standard palette."""
    text = f"{color_name} {title} {description}".lower()

    # Color mappings
    color_map = {
        "black": ["black", "noir", "onyx", "midnight"],
        "white": ["white", "ivory", "cream", "off-white", "bone"],
        "red": ["red", "crimson", "ruby", "burgundy", "wine", "maroon"],
        "blue": ["blue", "navy", "indigo", "cobalt", "sapphire", "denim"],
        "green": ["green", "olive", "forest", "emerald", "sage", "mint"],
        "yellow": ["yellow", "gold", "mustard", "lemon"],
        "orange": ["orange", "coral", "peach", "tangerine"],
        "pink": ["pink", "rose", "blush", "fuchsia", "magenta"],
        "purple": ["purple", "violet", "plum", "lavender", "lilac"],
        "brown": ["brown", "tan", "beige", "khaki", "camel", "chocolate"],
        "gray": ["gray", "grey", "silver", "charcoal", "slate"],
        "multicolor": ["multi", "rainbow", "tie-dye", "tie dye", "colorful"]
    }

    # Check for patterns first
    if any(kw in text for kw in ["stripe", "striped"]):
        return "Striped"
    if any(kw in text for kw in ["floral", "flower", "botanical"]):
        return "Floral"
    if any(kw in text for kw in ["print", "pattern", "geometric", "abstract"]):
        return "Patterned"

    # Match to color families
    for standard_color, keywords in color_map.items():
        if any(kw in text for kw in keywords):
            return standard_color.title()

    # Default
    return color_name.title() if color_name else "Neutral"

def detect_pattern_type(color_name, title, image_url=""):
    """Detect if product has a pattern and what type."""
    text = f"{color_name} {title}".lower()

    pattern_keywords = {
        "Stripe": ["stripe", "striped", "pinstripe"],
        "Floral": ["floral", "flower", "botanical", "bloom"],
        "Geometric": ["geometric", "chevron", "diamond", "grid"],
        "Abstract": ["abstract", "marble", "swirl", "tie-dye", "tie dye"],
        "Animal": ["leopard", "zebra", "snake", "cheetah"],
        "Plaid": ["plaid", "check", "checkered", "gingham"],
        "Polka Dot": ["polka", "dot", "dotted"],
    }

    for pattern, keywords in pattern_keywords.items():
        if any(kw in text for kw in keywords):
            return pattern

    return "Solid"

def hex_to_color_family(hex_code):
    """Map hex code to color family for filtering."""
    if not hex_code or hex_code == "":
        return "Neutral"

    try:
        # Remove # if present
        hex_code = hex_code.lstrip('#')

        # Convert to RGB
        r, g, b = tuple(int(hex_code[i:i+2], 16) for i in (0, 2, 4))

        # Calculate brightness
        brightness = (r + g + b) / 3

        # Check for grayscale
        if max(r, g, b) - min(r, g, b) < 30:
            if brightness < 50:
                return "Black"
            elif brightness > 200:
                return "White"
            else:
                return "Gray"

        # Determine dominant channel
        if r > g and r > b:
            if g > 100:
                return "Orange" if b < 100 else "Pink"
            return "Red"
        elif g > r and g > b:
            return "Green"
        elif b > r and b > g:
            if r > 100:
                return "Purple"
            return "Blue"

        return "Neutral"

    except Exception:
        return "Neutral"

def standardize_record(record):
    """Standardize a record to ensure consistent output format across all scrapers."""
    # Ensure all fields exist with proper defaults
    standardized = {}
    
    for field in CSV_FIELDNAMES:
        value = record.get(field, "")
        
        # Type-specific defaults
        if field in ["schema_version"]:
            standardized[field] = int(value) if value else SCHEMA_VERSION
        elif field in ["price", "natural_fiber_percent", "synthetic_fiber_percent"]:
            try:
                standardized[field] = float(value) if value else 0
            except (ValueError, TypeError):
                standardized[field] = 0
        elif field in ["is_apparel", "is_100pct_natural", "is_loomi_approved"]:
            standardized[field] = bool(value)
        else:
            # String fields - ensure no None values
            standardized[field] = str(value) if value is not None else ""
    
    # Normalize specific fields
    if standardized["color_name"]:
        standardized["color_name"] = standardized["color_name"].title()
    
    if standardized["category"]:
        standardized["category"] = standardized["category"].title()
    
    # Ensure price formatting is consistent
    if standardized["price"]:
        try:
            price_val = float(standardized["price"])
            standardized["price_raw"] = f"${price_val:.2f}"
            standardized["price"] = price_val
        except (ValueError, TypeError):
            pass
    
    # Normalize boolean fields to True/False strings for CSV
    for bool_field in ["is_apparel", "is_100pct_natural", "is_loomi_approved"]:
        standardized[bool_field] = str(standardized[bool_field])
    
    return standardized

def validate_row(row):
    """Validate a row for data quality issues."""
    errors = []

    # Check fiber math
    natural = row.get('natural_fiber_percent', 0)
    synthetic = row.get('synthetic_fiber_percent', 0)

    try:
        total = int(natural) + int(synthetic)
        if total > 105:  # Allow small rounding margin
            errors.append(f"Invalid fiber total: {total}%")
    except (ValueError, TypeError):
        pass

    # Check required fields
    required = ['audience', 'subcategory']
    for field in required:
        if not row.get(field):
            errors.append(f"Missing {field}")

    return errors

def save_to_csv(records, filename):
    """Save records to CSV with validation and standardization."""
    if not records:
        logger.warning("No records to save")
        return

    # Standardize all records first
    standardized_records = [standardize_record(r) for r in records]

    # Validate records
    validation_errors = []
    for i, record in enumerate(standardized_records):
        errors = validate_row(record)
        if errors:
            validation_errors.append(f"Row {i}: {', '.join(errors)}")

    if validation_errors:
        logger.warning(f"\n⚠️  Validation warnings ({len(validation_errors)} rows):")
        for error in validation_errors[:10]:  # Show first 10
            logger.warning(f"  {error}")

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(standardized_records)

    logger.info(f"✓ Saved {len(records)} records to {filename}")

# =============================================================================
# SHOPIFY PARSER (Buddha Pants)
# =============================================================================

def extract_shopify_product_json(soup, product_url):
    """Extract product data from Shopify-based sites (Buddha Pants)."""
    product_json = None

    # Method 1: Look for <script id="ProductJson-*"> tags (common Shopify pattern)
    for script in soup.find_all('script', id=re.compile(r'ProductJson')):
        try:
            if script.string:
                product_json = json.loads(script.string)
                logger.debug(f"Found product JSON in script#ProductJson")
                break
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    # Method 2: Look for window.ShopifyAnalytics.meta.product
    if not product_json:
        for script in soup.find_all('script'):
            try:
                text = script.string or ''
                # Look for ShopifyAnalytics pattern
                if 'ShopifyAnalytics.meta.product' in text:
                    match = re.search(r'ShopifyAnalytics\.meta\.product\s*=\s*({.+?});', text, re.DOTALL)
                    if match:
                        product_json = json.loads(match.group(1))
                        logger.debug(f"Found product JSON in ShopifyAnalytics")
                        break
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue

    # Method 3: Look for application/ld+json (Schema.org) and convert
    if not product_json:
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                if not script.string:
                    continue
                    
                ld_json = json.loads(script.string)
                if not isinstance(ld_json, dict):
                    continue
                    
                if ld_json.get('@type') == 'Product':
                    # Convert Schema.org format to Shopify-like format
                    product_json = {
                        'title': ld_json.get('name', ''),
                        'id': hashlib.md5(product_url.encode()).hexdigest()[:12],
                        'type': '',
                        'description': ld_json.get('description', ''),
                        'variants': [],
                        'price_currency_code': 'USD'
                    }

                    # Extract variants from offers
                    offers = ld_json.get('offers', {})
                    if isinstance(offers, list):
                        for offer in offers:
                            product_json['variants'].append({
                                'id': hashlib.md5(str(offer.get('url', product_url)).encode()).hexdigest()[:12],
                                'title': offer.get('name', ld_json.get('name', '')),
                                'price': int(float(offer.get('price', 0)) * 100),
                                'featured_image': {'src': ld_json.get('image', '')}
                            })
                        if offers:
                            product_json['price_currency_code'] = offers[0].get('priceCurrency', 'USD')
                    elif isinstance(offers, dict):
                        product_json['variants'].append({
                            'id': hashlib.md5(product_url.encode()).hexdigest()[:12],
                            'title': ld_json.get('name', ''),
                            'price': int(float(offers.get('price', 0)) * 100),
                            'featured_image': {'src': ld_json.get('image', '')}
                        })
                        product_json['price_currency_code'] = offers.get('priceCurrency', 'USD')
                    
                    logger.debug(f"Found product JSON in ld+json")
                    break
            except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
                continue

    return product_json


def scrape_product_page(session, product_url, robot_parser, config):
    """Scrape a single product page and return list of color variant records."""
    if not can_fetch(robot_parser, product_url):
        return []

    try:
        resp = session.get(product_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        # Extract product JSON using Shopify-specific method
        product_json = extract_shopify_product_json(soup, product_url)

        if not product_json:
            logger.warning(f"No product JSON found for {product_url}")
            return []

        # Extract base fields
        title = product_json.get('title', '')
        product_id = str(product_json.get('id', ''))
        style_slug = product_url.split('/products/')[-1].split('?')[0]
        style_id = hashlib.md5(style_slug.encode()).hexdigest()[:12]

        # Extract category
        product_type = product_json.get('type', '')
        category = product_type if product_type else "Apparel"
        
        # Detect non-apparel items from title
        title_lower = title.lower()
        if any(kw in title_lower for kw in ['journal', 'notebook', 'notes', 'sticker', 'paper']):
            category = "Accessories"
            is_apparel = False
        elif any(kw in title_lower for kw in ['bag', 'purse', 'fanny', 'clutch', 'pouch']):
            category = "Accessories"
            is_apparel = False
        elif any(kw in title_lower for kw in ['scarf', 'lanyard', 'keychain']):
            category = "Accessories"
            is_apparel = False
        else:
            is_apparel = category.lower() not in ['accessories', 'bags', 'jewelry']

        # Extract materials
        description_html = product_json.get('description', '')
        materials_text = BeautifulSoup(description_html, 'lxml').get_text(separator=' ', strip=True)
        materials_snippet = materials_text[:200] if materials_text else ""

        # Fallback: check title for material mentions if description is empty
        if not materials_text and title:
            title_lower = title.lower()
            # Common patterns in titles
            if '100% organic cotton' in title_lower or '100% cotton' in title_lower:
                materials_text = "100% Organic Cotton"
            elif 'cotton' in title_lower and 'linen' in title_lower:
                materials_text = "Cotton/Linen blend"
            elif 'organic cotton' in title_lower or 'cotton' in title_lower:
                materials_text = "Cotton"

        # Parse fabric - returns (breakdown_string, tags_list)
        fabric_breakdown_pretty, fabric_tags_list = parse_fabric_breakdown(materials_text)
        fabric_breakdown_pretty = fabric_breakdown_pretty or ""

        # Analyze fibers
        fiber_analysis = analyze_fibers(fabric_breakdown_pretty)

        # Extract Loomi fields
        audience = infer_audience(title, category)
        subcategory = map_subcategory(category, title)
        sizes_available = extract_sizes(soup, resp.text)
        waist_sizes = extract_waist_sizes(sizes_available, subcategory)
        length = extract_length(title, resp.text)
        care_level = determine_care_level(fiber_analysis.get('fiber_families', ''))
        fabric_tags = generate_fabric_tags(fiber_analysis, fabric_breakdown_pretty)
        occasion_tag = determine_occasion(title, category, subcategory)
        description = generate_description(title, materials_snippet, category)

        # Extract gallery images
        gallery_images = extract_gallery_images(soup, config["base_url"], config, style_slug)

        # Process variants (colors)
        variants = product_json.get('variants', [])
        records = []

        for variant in variants:
            variant_id = str(variant.get('id', ''))
            color_name = variant.get('option1') or variant.get('title', 'Default')

            # Skip variants that seem to be just color swatches or decorative
            if color_name.lower() in ["select color", "choose color", "color", "default title", "default"]:
                color_name = title
                if title.lower() == color_name.lower():
                    logger.debug(f"Skipping duplicate title as color name: {product_url}")
                    continue

            color_id = f"{style_id}-{hashlib.md5(color_name.encode()).hexdigest()[:12]}"

            # Price
            price_cents = variant.get('price', 0)
            price = float(price_cents) / 100 if price_cents else 0.0
            currency = product_json.get('price_currency_code', 'USD')

            # Image - handle case where 'src' is a dict with 'url' key (Shopify Schema.org format)
            image_url = ""
            if variant.get('featured_image'):
                src = variant['featured_image'].get('src', '')
                if isinstance(src, dict):
                    src = src.get('url', '') or src.get('image', '')
                image_url = normalize_image_url(src, config["base_url"]) if src else ""
            elif gallery_images:
                image_url = gallery_images[0]

            # Scrape status
            scrape_status = "ok"
            if not price:
                scrape_status = "missing_price"
            elif not materials_text:
                scrape_status = "missing_materials"

            # Color analysis - SKIPPED for speed (defer to post-processing)
            dominant_hex = ""  # extract_dominant_color_hex(image_url) - slow, download+analyze
            color_name_normalized = normalize_color_name(color_name, title, materials_text)
            pattern_type = detect_pattern_type(color_name, title, image_url)
            color_family = color_name_normalized  # Skip hex_to_color_family since no hex

            # Extract extended product details with error handling
            try:
                sku = extract_sku(soup, resp.text, product_url)
                fit_type = extract_fit_type(title, materials_text, category)
                care_instructions = extract_care_instructions(soup, materials_text)
                sustainability_tags = extract_sustainability_info(soup, resp.text)
                country_of_origin = extract_country_of_origin(soup, resp.text)
                season = extract_season(title, category, resp.text)
            except Exception as e:
                logger.debug(f"Error extracting extended details: {e}")
                sku = fit_type = care_instructions = ""
                sustainability_tags = country_of_origin = season = ""

            record = {
                "schema_version": SCHEMA_VERSION,
                "style_id": style_id,
                "style_slug": style_slug,
                "color_id": color_id,
                "color_name": color_name,
                "color_name_normalized": color_name_normalized,
                "dominant_hex": dominant_hex,
                "pattern_type": pattern_type,
                "color_family": color_family,
                "image_url": image_url,
                "gallery_image_urls": "|".join(gallery_images),
                "product_url": product_url,
                "brand_name": "Buddha Pants",
                "source_site": "www.buddhapants.com",
                "product_title": title,
                "category": category,
                "is_apparel": is_apparel,
                "price_raw": f"${price:.2f}" if price else "",
                "price": price,
                "currency": currency,
                "materials_raw_or_page_text": materials_text,
                "materials_snippet": materials_snippet,
                "scrape_status": scrape_status,
                # Loomi fields
                "audience": audience,
                "subcategory": subcategory,
                "sizes_available": sizes_available,
                "waist_sizes": waist_sizes,
                "length": length,
                "natural_fiber_percent": fiber_analysis.get("natural_fiber_percent", 0),
                "synthetic_fiber_percent": fiber_analysis.get("synthetic_fiber_percent", 0),
                "is_100pct_natural": fiber_analysis.get("is_100pct_natural", False),
                "is_loomi_approved": fiber_analysis.get("is_loomi_approved", False),
                "fiber_families": fiber_analysis.get("fiber_families", ""),
                "synthetic_fibers_present": fiber_analysis.get("synthetic_fibers_present", ""),
                "care_level": care_level,
                "description": description,
                "fabric_breakdown_pretty": fabric_breakdown_pretty or "",
                "fabric_tags": fabric_tags,
                "occasion_tag": occasion_tag,
                # Extended details
                "sku": sku,
                "color_code": variant_id,
                "fit_type": fit_type,
                "care_instructions": care_instructions,
                "product_weight": "",
                "country_of_origin": country_of_origin,
                "sustainability_tags": sustainability_tags,
                "season": season,
                "collection_name": ""
            }

            records.append(record)

        # Deduplicate by color_name (important for some Shopify sites with duplicate variants)
        seen = {}
        for r in records:
            key = (r['style_id'], r['color_name'])
            if key not in seen:
                seen[key] = r

        return list(seen.values())

    except Exception as e:
        logger.error(f"Error scraping product page: {e}")
        return []

# =============================================================================
# COLLECTION SCRAPER
# =============================================================================

def get_product_links_from_collection(session, collection_url, robot_parser, config):
    """Extract product links from a collection page."""
    if not can_fetch(robot_parser, collection_url):
        logger.warning(f"Robots.txt blocks access to {collection_url}")
        return set()

    try:
        resp = session.get(collection_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')

        links = set()
        
        # Try multiple selectors for Buddha Pants
        selectors_to_try = [
            config["selectors"]["product_links"],
            "a[href*='/products/']",  # Any link containing /products/
            ".product-item a",
            ".grid-product__link",
            "a.product-link"
        ]
        
        for selector in selectors_to_try:
            elements = soup.select(selector)
            logger.debug(f"Selector '{selector}' found {len(elements)} elements")
            
            for a in elements:
                href = a.get('href', '')
                if isinstance(href, str) and '/products/' in href:
                    full_url = urljoin(config["base_url"], href)
                    links.add(full_url)
            
            if links:
                logger.info(f"Found {len(links)} products using selector: {selector}")
                break
        
        if not links:
            logger.warning(f"No products found with any selector. Dumping first 500 chars of HTML:")
            logger.warning(resp.text[:500])

        return links

    except Exception as e:
        logger.error(f"Error extracting product links: {e}")
        return set()

# =============================================================================
# BANANA REPUBLIC PARSER
# =============================================================================

def extract_banana_republic_product_json(soup, product_url):
    """Extract product data from Banana Republic specific JSON structure."""
    styles_data = None
    for script in soup.find_all('script'):
        text = script.string or ''
        if 'window.gap.properties.styles' in text:
            match = re.search(r'window\.gap\.properties\.styles\s*=\s*(\{.+?\});', text, re.DOTALL)
            if match:
                try:
                    styles_data = json.loads(match.group(1))
                    logger.debug(f"Found Banana Republic styles data")
                    break
                except json.JSONDecodeError:
                    logger.warning(f"Could not decode Banana Republic styles JSON.")
                    continue
    return styles_data


def scrape_banana_republic_product(html, soup, product_url, config):
    """Parse Banana Republic product page."""
    records = []

    try:
        # Extract styles JSON
        styles_data = extract_banana_republic_product_json(soup, product_url)

        if not styles_data:
            logger.warning(f"No Banana Republic styles data found for {product_url}")
            return []

        # Extract product metadata from schema.org JSON-LD
        price = 0.0
        currency = "USD"
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                ld_json = json.loads(script.string)
                if ld_json.get('@type') == 'Product':
                    offers = ld_json.get('offers', {})
                    price = float(offers.get('price', 0))
                    currency = offers.get('priceCurrency', 'USD')
                    break
            except (json.JSONDecodeError, ValueError, AttributeError):
                continue

        # Extract title and description
        title_tag = soup.find('h1', class_='product-name')
        title = title_tag.get_text(strip=True) if title_tag else "Unknown Product"

        desc_div = soup.find('div', class_='product-details__description')
        materials_text = desc_div.get_text(separator=' ', strip=True) if desc_div else ""
        materials_snippet = materials_text[:200] if materials_text else ""

        # Parse fabric - returns (breakdown_string, tags_list)
        fabric_breakdown_pretty, fabric_tags_list = parse_fabric_breakdown(materials_text)
        fabric_breakdown_pretty = fabric_breakdown_pretty or ""

        # Analyze fibers
        fiber_analysis = analyze_fibers(fabric_breakdown_pretty)

        # Extract category
        category = "Sweaters"  # Default, likely overridden by actual product type if available
        product_type_tag = soup.find('a', class_='breadcrumbs__crumb', href=lambda href: href and '/browse/womens-clothing' in href)
        if product_type_tag:
            category = product_type_tag.get_text(strip=True)

        is_apparel = category.lower() not in ['accessories', 'bags', 'jewelry']


        # Extract Loomi fields
        audience = infer_audience(title, category)
        subcategory = map_subcategory(category, title)
        sizes_available = extract_sizes(soup, html)
        waist_sizes = extract_waist_sizes(sizes_available, subcategory)
        length = extract_length(title, html)
        care_level = determine_care_level(fiber_analysis.get('fiber_families', ''))
        fabric_tags = generate_fabric_tags(fiber_analysis, fabric_breakdown_pretty)
        occasion_tag = determine_occasion(title, category, subcategory)
        description = generate_description(title, materials_snippet, category)

        # Process each style/color variant
        for style_id_str, style_data in styles_data.items():
            style_slug = style_id_str
            color_name = style_data.get('displayName', 'Default')

            # Generate IDs
            style_hash = hashlib.md5(style_id_str.encode()).hexdigest()[:12]
            color_id = f"{style_hash}-{hashlib.md5(color_name.encode()).hexdigest()[:12]}"

            # Extract images
            # Use the `style_id_str` for image selectors if available, else fallback
            gallery_images = extract_gallery_images(soup, config["base_url"], config, style_id_str)
            if not gallery_images and style_data.get('images'):
                for img_data in style_data['images']:
                    img_url = normalize_image_url(img_data.get('url'), config["base_url"])
                    if img_url and img_url not in gallery_images:
                        gallery_images.append(img_url)

            image_url = gallery_images[0] if gallery_images else ""


            # Scrape status
            scrape_status = "ok"
            if not price:
                scrape_status = "missing_price"
            elif not materials_text:
                scrape_status = "missing_materials"

            # Color analysis - SKIPPED for speed (defer to post-processing)
            dominant_hex = ""  # extract_dominant_color_hex(image_url) - slow, download+analyze
            color_name_normalized = normalize_color_name(color_name, title, materials_text)
            pattern_type = detect_pattern_type(color_name, title, image_url)
            color_family = color_name_normalized  # Skip hex_to_color_family since no hex

            record = {
                "schema_version": SCHEMA_VERSION,
                "style_id": style_hash,
                "style_slug": style_slug,
                "color_id": color_id,
                "color_name": color_name,  # Will be title-cased by standardize_record
                "color_name_normalized": color_name_normalized,
                "dominant_hex": dominant_hex,
                "pattern_type": pattern_type,
                "color_family": color_family,
                "image_url": image_url,
                "gallery_image_urls": "|".join(gallery_images),
                "product_url": product_url,
                "brand_name": "Banana Republic",
                "source_site": "bananarepublic.gap.com",
                "product_title": title,
                "category": category,  # Will be title-cased by standardize_record
                "is_apparel": is_apparel,
                "price_raw": f"${price:.2f}" if price else "",
                "price": price,
                "currency": currency,
                "materials_raw_or_page_text": materials_text,
                "materials_snippet": materials_snippet,
                "scrape_status": scrape_status,
                # Loomi fields
                "audience": audience,
                "subcategory": subcategory,
                "sizes_available": sizes_available,
                "waist_sizes": waist_sizes,
                "length": length,
                "natural_fiber_percent": fiber_analysis.get("natural_fiber_percent", 0),
                "synthetic_fiber_percent": fiber_analysis.get("synthetic_fiber_percent", 0),
                "is_100pct_natural": fiber_analysis.get("is_100pct_natural", False),
                "is_loomi_approved": fiber_analysis.get("is_loomi_approved", False),
                "fiber_families": fiber_analysis.get("fiber_families", ""),
                "synthetic_fibers_present": fiber_analysis.get("synthetic_fibers_present", ""),
                "care_level": care_level,
                "description": description,
                "fabric_breakdown_pretty": fabric_breakdown_pretty or "",
                "fabric_tags": fabric_tags,
                "occasion_tag": occasion_tag
            }

            records.append(record)

        return records

    except Exception as e:
        logger.error(f"Error parsing BR product: {e}")
        return []

# =============================================================================
# MAIN SCRAPER
# =============================================================================

def run_banana_republic_scraper(config):
    """Scrape Banana Republic using Playwright (browser required)."""
    # BR uses Next.js with client-side rendering - product links are rendered by JavaScript
    # Replit sandbox lacks system libraries for headless browser
    if os.environ.get("REPL_ID"):
        print("\n" + "=" * 70)
        print("⚠️  Banana Republic Scraper - Replit Limitation")
        print("=" * 70)
        print("\nBanana Republic uses Next.js (client-side rendering) with JavaScript.")
        print("Product links are generated dynamically and NOT available in static HTML.")
        print("\n✓ Buddha Pants scraper works in Replit: python main.py buddhapants")
        print("✓ BR scraping requires either:")
        print("  • Running locally with Playwright installed")
        print("  • Using Gap API (if publicly available)")
        print("\nFor local setup, see LOCAL_BANANA_REPUBLIC_SETUP.md")
        print("=" * 70 + "\n")
        return

    # Legacy code preserved below (would work if HTML contained product links)
    logger.info("=" * 70)
    logger.info(f"Starting Loomi Scraper for {config['name']} (requests + JSON mode)")
    logger.info("=" * 70)

    session = get_session()
    all_product_urls = set()
    all_records = []
    failed_products = 0

    # Extract product links from collection pages using requests (no browser needed)
    for collection_url in config["collection_urls"]:
        logger.info(f"\nProcessing collection: {collection_url}")
        try:
            resp = session.get(collection_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')

            # Extract product links from the HTML
            found = 0
            for selector in config["selectors"]["product_links"].split(", "):
                links = soup.select(selector)
                if links:
                    logger.info(f"  Found {len(links)} products using selector: {selector}")
                    for link in links:
                        href = link.get('href', '')
                        if isinstance(href, str) and href:
                            full_url = urljoin(config["base_url"], str(href))
                            if '/browse/product' in full_url or '/p/' in full_url or 'pid=' in full_url:
                                all_product_urls.add(full_url)
                                found += 1
                    if found > 0:
                        break

            logger.info(f"  Added {len(all_product_urls)} total unique products so far")

            # Limit per collection to avoid timeouts
            if len(all_product_urls) >= 100:
                logger.info("  Reached product collection limit (100). Moving to scraping phase.")
                break

        except Exception as e:
            logger.warning(f"  Error processing collection {collection_url}: {e}")
            continue

    logger.info(f"\nTotal unique product URLs to scrape: {len(all_product_urls)}")

    # Scrape each product using requests (no browser)
    for i, product_url in enumerate(sorted(all_product_urls), 1):
        try:
            logger.info(f"\n[{i}/{len(all_product_urls)}]")
            resp = session.get(product_url, timeout=30)
            resp.raise_for_status()

            html = resp.text
            soup = BeautifulSoup(html, 'lxml')

            # Use existing BR extraction function
            records = scrape_banana_republic_product(html, soup, product_url, config)

            if records:
                all_records.extend(records)
                logger.info(f"  Extracted {len(records)} color variants.")
                if len(records) > 0:
                    r = records[0]
                    logger.info(f"  First variant: {r['color_name']:25} | {r['product_title'][:30]} | {r['scrape_status']}")
            else:
                failed_products += 1
                logger.warning(f"  No records extracted from {product_url}")

            # Random delay between requests
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            failed_products += 1
            logger.warning(f"  Error scraping {product_url}: {e}")
            continue

    save_to_csv(all_records, config["output_file"])

    logger.info("\n" + "=" * 70)
    logger.info("SCRAPING COMPLETE")
    logger.info(f"Total color variants scraped: {len(all_records)}")
    logger.info(f"Failed products (could not extract data): {failed_products}")
    logger.info(f"Unique products processed: {len(all_product_urls)}")
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
    if config.get("parser_type") == "banana_republic":
        return run_banana_republic_scraper(config)
    elif config.get("parser_type") == "shopify":
        logger.info("=" * 70)
        logger.info(f"Starting Loomi Scraper for {config['name']} (requests mode)")
        logger.info("=" * 70)

        session = get_session()
        robot_parser = check_robots_txt(config["base_url"])

        all_product_urls = set()
        for collection_url in config["collection_urls"]:
            logger.info(f"\nProcessing collection: {collection_url}")
            urls = get_product_links_from_collection(session, collection_url, robot_parser, config)
            all_product_urls.update(urls)

        logger.info(f"\nTotal unique products: {len(all_product_urls)}")

        all_records = []
        failed_products = 0
        for i, product_url in enumerate(sorted(all_product_urls), 1):
            logger.info(f"\n[{i}/{len(all_product_urls)}]")
            records = scrape_product_page(session, product_url, robot_parser, config)
            if records:
                all_records.extend(records)
                logger.info(f"  Extracted {len(records)} color variants.")
                if len(records) > 0:
                    r = records[0]
                    logger.info(f"  First variant: {r['color_name']:25} | {r['product_title'][:30]} | {r['scrape_status']}")
            else:
                failed_products += 1
                logger.warning(f"No records extracted from {product_url}")

            # Add a small random delay between requests
            time.sleep(random.uniform(0.5, 1.5))

        save_to_csv(all_records, config["output_file"])

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
    else:
        logger.error(f"Unknown parser_type '{config.get('parser_type')}' for site {config['name']}")


def get_extract_function(site_key: str):
    """Get the appropriate extraction function for a site."""
    if site_key == "buddhapants":
        return scrape_product_page_extract
    elif site_key == "bananarepublic":
        return scrape_banana_republic_product
    else:
        return scrape_product_page_extract


def scrape_product_page_extract(html, soup, product_url, config):
    """Extract product data directly from provided HTML/soup (no refetch)."""
    try:
        # Extract product JSON using Shopify-specific method
        product_json = extract_shopify_product_json(soup, product_url)

        if not product_json:
            logger.warning(f"No product JSON found for {product_url}")
            return []

        # Extract base fields
        title = product_json.get('title', '')
        product_id = str(product_json.get('id', ''))
        style_slug = product_url.split('/products/')[-1].split('?')[0]
        style_id = hashlib.md5(style_slug.encode()).hexdigest()[:12]

        # Extract category
        product_type = product_json.get('type', '')
        category = product_type if product_type else "Apparel"
        
        # Detect non-apparel items from title
        title_lower = title.lower()
        if any(kw in title_lower for kw in ['journal', 'notebook', 'notes', 'sticker', 'paper']):
            category = "Accessories"
            is_apparel = False
        elif any(kw in title_lower for kw in ['bag', 'purse', 'fanny', 'clutch', 'pouch']):
            category = "Accessories"
            is_apparel = False
        elif any(kw in title_lower for kw in ['scarf', 'lanyard', 'keychain']):
            category = "Accessories"
            is_apparel = False
        else:
            is_apparel = category.lower() not in ['accessories', 'bags', 'jewelry']

        # Extract materials
        description_html = product_json.get('description', '')
        materials_text = BeautifulSoup(description_html, 'lxml').get_text(separator=' ', strip=True)
        materials_snippet = materials_text[:200] if materials_text else ""

        # Fallback: check title for material mentions if description is empty
        if not materials_text and title:
            title_lower = title.lower()
            if '100% organic cotton' in title_lower or '100% cotton' in title_lower:
                materials_text = "100% Organic Cotton"
            elif 'cotton' in title_lower and 'linen' in title_lower:
                materials_text = "Cotton/Linen blend"
            elif 'organic cotton' in title_lower or 'cotton' in title_lower:
                materials_text = "Cotton"

        # Parse fabric
        fabric_breakdown_pretty, fabric_tags_list = parse_fabric_breakdown(materials_text)
        fabric_breakdown_pretty = fabric_breakdown_pretty or ""

        # Analyze fibers
        fiber_analysis = analyze_fibers(fabric_breakdown_pretty)

        # Extract Loomi fields
        audience = infer_audience(title, category)
        subcategory = map_subcategory(category, title)
        sizes_available = extract_sizes(soup, html)
        waist_sizes = extract_waist_sizes(sizes_available, subcategory)
        length = extract_length(title, html)
        care_level = determine_care_level(fiber_analysis.get('fiber_families', ''))
        fabric_tags = generate_fabric_tags(fiber_analysis, fabric_breakdown_pretty)
        occasion_tag = determine_occasion(title, category, subcategory)
        description = generate_description(title, materials_snippet, category)

        # Extract gallery images
        gallery_images = extract_gallery_images(soup, config["base_url"], config, style_slug)

        # Process variants (colors)
        variants = product_json.get('variants', [])
        records = []

        for variant in variants:
            variant_id = str(variant.get('id', ''))
            color_name = variant.get('option1') or variant.get('title', 'Default')

            # Skip variants that seem to be just color swatches or decorative
            if color_name.lower() in ["select color", "choose color", "color", "default title", "default"]:
                color_name = title
                if title.lower() == color_name.lower():
                    logger.debug(f"Skipping duplicate title as color name: {product_url}")
                    continue

            color_id = f"{style_id}-{hashlib.md5(color_name.encode()).hexdigest()[:12]}"

            # Price
            price_cents = variant.get('price', 0)
            price = float(price_cents) / 100 if price_cents else 0.0
            currency = product_json.get('price_currency_code', 'USD')

            # Image
            image_url = ""
            if variant.get('featured_image'):
                src = variant['featured_image'].get('src', '')
                if isinstance(src, dict):
                    src = src.get('url', '') or src.get('image', '')
                image_url = normalize_image_url(src, config["base_url"]) if src else ""
            elif gallery_images:
                image_url = gallery_images[0]

            # Scrape status
            scrape_status = "ok"
            if not price:
                scrape_status = "missing_price"
            elif not materials_text:
                scrape_status = "missing_materials"

            # Color analysis - SKIPPED for speed (defer to post-processing)
            dominant_hex = ""  # extract_dominant_color_hex(image_url) - slow, download+analyze
            color_name_normalized = normalize_color_name(color_name, title, materials_text)
            pattern_type = detect_pattern_type(color_name, title, image_url)
            color_family = color_name_normalized  # Skip hex_to_color_family since no hex

            # Extract extended product details with error handling
            try:
                sku = extract_sku(soup, html, product_url)
                fit_type = extract_fit_type(title, materials_text, category)
                care_instructions = extract_care_instructions(soup, materials_text)
                sustainability_tags = extract_sustainability_info(soup, html)
                country_of_origin = extract_country_of_origin(soup, html)
                season = extract_season(title, category, html)
            except Exception as e:
                logger.debug(f"Error extracting extended details: {e}")
                sku = fit_type = care_instructions = ""
                sustainability_tags = country_of_origin = season = ""

            record = {
                "schema_version": SCHEMA_VERSION,
                "style_id": style_id,
                "style_slug": style_slug,
                "color_id": color_id,
                "color_name": color_name,
                "color_name_normalized": color_name_normalized,
                "dominant_hex": dominant_hex,
                "pattern_type": pattern_type,
                "color_family": color_family,
                "image_url": image_url,
                "gallery_image_urls": "|".join(gallery_images),
                "product_url": product_url,
                "brand_name": "Buddha Pants",
                "source_site": "www.buddhapants.com",
                "product_title": title,
                "category": category,
                "is_apparel": is_apparel,
                "price_raw": f"${price:.2f}" if price else "",
                "price": price,
                "currency": currency,
                "materials_raw_or_page_text": materials_text,
                "materials_snippet": materials_snippet,
                "scrape_status": scrape_status,
                "audience": audience,
                "subcategory": subcategory,
                "sizes_available": sizes_available,
                "waist_sizes": waist_sizes,
                "length": length,
                "natural_fiber_percent": fiber_analysis.get("natural_fiber_percent", 0),
                "synthetic_fiber_percent": fiber_analysis.get("synthetic_fiber_percent", 0),
                "is_100pct_natural": fiber_analysis.get("is_100pct_natural", False),
                "is_loomi_approved": fiber_analysis.get("is_loomi_approved", False),
                "fiber_families": fiber_analysis.get("fiber_families", ""),
                "synthetic_fibers_present": fiber_analysis.get("synthetic_fibers_present", ""),
                "care_level": care_level,
                "description": description,
                "fabric_breakdown_pretty": fabric_breakdown_pretty or "",
                "fabric_tags": fabric_tags,
                "occasion_tag": occasion_tag,
                "sku": sku,
                "color_code": variant_id,
                "fit_type": fit_type,
                "care_instructions": care_instructions,
                "product_weight": "",
                "country_of_origin": country_of_origin,
                "sustainability_tags": sustainability_tags,
                "season": season,
                "collection_name": ""
            }

            records.append(record)

        # Deduplicate by color_name
        seen = {}
        for r in records:
            key = (r['style_id'], r['color_name'])
            if key not in seen:
                seen[key] = r

        return list(seen.values())

    except Exception as e:
        logger.error(f"Error extracting product data: {e}")
        return []


def run_with_manager(site_key: str):
    """Run scraper using the new ScrapeManager architecture."""
    try:
        site_profile = get_site_profile(site_key)
    except ValueError as e:
        print(f"Error: {e}")
        print(f"Available sites: {', '.join(list_sites())}")
        return
    
    extract_fn = get_extract_function(site_key)
    
    manager = ScrapeManager(site_profile, extract_fn)
    records = manager.run()
    
    if records:
        save_to_csv(records, site_profile.output_file)
        print(f"\n✓ Saved {len(records)} records to {site_profile.output_file}")
    else:
        print(f"\nNo records scraped for {site_profile.name}")


def main():
    """Main entry point for the scraper."""
    site_key = "buddhapants"
    if len(sys.argv) > 1:
        site_key = sys.argv[1].lower()
    
    if site_key == "--list":
        print("Available sites:")
        for key in list_sites():
            profile = get_site_profile(key)
            strategies = [s.name for s in profile.strategies]
            print(f"  {key}: {profile.name} (strategies: {', '.join(strategies)})")
        return
    
    if site_key == "--env":
        env = EnvironmentProbe()
        print("Environment capabilities:")
        for cap, val in env.get_capabilities().items():
            status = "✓" if val else "✗"
            print(f"  {status} {cap}: {val}")
        return
    
    print(f"\nLoomi Scraper – running for: {site_key}\n")
    
    if site_key in SITE_PROFILES:
        run_with_manager(site_key)
    elif site_key in SITE_CONFIGS:
        config = SITE_CONFIGS[site_key]
        print(f"(Using legacy scraper for {config['name']})")
        run_scraper(config)
    else:
        print(f"Unknown site '{site_key}'.")
        print(f"Available sites: {', '.join(list_sites())}")
        print("\nCommands:")
        print("  python main.py --list  # List all sites and strategies")
        print("  python main.py --env   # Show environment capabilities")


if __name__ == "__main__":
    main()