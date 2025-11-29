"""
Site Profiles - Configuration for all supported e-commerce sites
=================================================================
Each site has a SiteProfile with strategies listed in priority order.
The ScrapeManager will try strategies in order until one succeeds.

To add a new site:
1. Create a SiteProfile with site metadata
2. Add strategies in priority order (first = preferred)
3. Register it in SITE_PROFILES dict
4. Implement extraction function if needed (or use default)
"""

from scraper_engine import (
    SiteProfile,
    RequestsStrategy,
    PlaywrightStrategy,
)


# =============================================================================
# BUDDHA PANTS (Shopify-based)
# =============================================================================
# Works with requests only - Shopify embeds product JSON in HTML

BUDDHA_PANTS = SiteProfile(
    name="Buddha Pants",
    key="buddhapants",
    base_url="https://www.buddhapants.com",
    collection_urls=[
        "https://www.buddhapants.com/collections/pants",
        "https://www.buddhapants.com/collections/tops",
        "https://www.buddhapants.com/collections/jumpsuits",
    ],
    output_file="buddhapants_raw.csv",
    brand_name="Buddha Pants",
    source_site="www.buddhapants.com",
    strategies=[
        RequestsStrategy(request_delay=0.3),
    ],
    selectors={
        "product_links": "a.product-item__title, a[href*='/products/']",
        "product_title": "h1.product__title",
        "price": "span.price-item--regular",
        "materials": "div.product__description",
        "gallery_images": "div.product__media img",
    },
    product_url_patterns=["/products/"],
    extract_function_name="shopify",
)


# =============================================================================
# BANANA REPUBLIC (Next.js - requires browser)
# =============================================================================
# Needs Playwright for collection pages (JS-rendered)
# Falls back to showing helpful message if browser unavailable

BANANA_REPUBLIC = SiteProfile(
    name="Banana Republic",
    key="bananarepublic",
    base_url="https://bananarepublic.gap.com",
    collection_urls=[
        "https://bananarepublic.gap.com/browse/women?cid=5002",
        "https://bananarepublic.gap.com/browse/women/dresses-and-jumpsuits?cid=69883",
        "https://bananarepublic.gap.com/browse/women/pants?cid=67595",
        "https://bananarepublic.gap.com/browse/women/sweaters?cid=5032",
        "https://bananarepublic.gap.com/browse/women/workwear?cid=1178917",
        "https://bananarepublic.gap.com/browse/women/accessories?cid=1134528",
        "https://bananarepublic.gap.com/browse/men?cid=5343",
        "https://bananarepublic.gap.com/browse/sale?cid=1014329",
    ],
    output_file="bananarepublic_raw.csv",
    brand_name="Banana Republic",
    source_site="bananarepublic.gap.com",
    strategies=[
        PlaywrightStrategy(scroll_count=10, page_timeout=60000),
    ],
    selectors={
        "product_links": "a.product-card__link, a[href*='/browse/product.do?pid=']",
        "product_title": "h1.product-name",
        "price": "span.product-price__highlight",
        "materials": "div.product-details__description",
        "gallery_images": "div.pdp-photo-single-column-image img",
    },
    product_url_patterns=["/browse/product", "pid="],
    extract_function_name="banana_republic",
)


# =============================================================================
# SITE REGISTRY
# =============================================================================

SITE_PROFILES = {
    "buddhapants": BUDDHA_PANTS,
    "bananarepublic": BANANA_REPUBLIC,
}


def get_site_profile(key: str) -> SiteProfile:
    """Get a site profile by key."""
    if key not in SITE_PROFILES:
        available = ", ".join(SITE_PROFILES.keys())
        raise ValueError(f"Unknown site '{key}'. Available: {available}")
    return SITE_PROFILES[key]


def list_sites() -> list:
    """Return list of available site keys."""
    return list(SITE_PROFILES.keys())
