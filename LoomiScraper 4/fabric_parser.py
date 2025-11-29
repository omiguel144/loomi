import re

# Known fiber keywords to detect real fiber phrases
FIBER_KEYWORDS = [
    "cotton",
    "organic cotton",
    "linen",
    "flax",
    "wool",
    "merino",
    "cashmere",
    "alpaca",
    "silk",
    "hemp",
    "bamboo",
    "viscose",
    "rayon",
    "modal",
    "polyester",
    "nylon",
    "spandex",
    "elastane",
    "acrylic",
]

# Map raw phrases to normalized tags
def normalize_fiber_family(base: str) -> str:
    b = base.lower()
    if "organic cotton" in b or "cotton" in b:
        return "COTTON"
    if "linen" in b or "flax" in b:
        return "LINEN"
    if "merino" in b:
        return "MERINO_WOOL"
    if "wool" in b:
        return "WOOL"
    if "cashmere" in b:
        return "CASHMERE"
    if "alpaca" in b:
        return "ALPACA"
    if "silk" in b:
        return "SILK"
    if "hemp" in b:
        return "HEMP"
    if "bamboo" in b:
        return "BAMBOO"
    if "viscose" in b or "rayon" in b or "modal" in b:
        return "REGENERATED_CELLULOSE"
    if "polyester" in b:
        return "POLYESTER"
    if "nylon" in b:
        return "NYLON"
    if "spandex" in b or "elastane" in b:
        return "ELASTANE"
    if "acrylic" in b:
        return "ACRYLIC"
    return base.strip().upper()


def is_fiber_phrase(text: str) -> bool:
    t = text.lower()
    return any(word in t for word in FIBER_KEYWORDS)


def clean_fiber_phrase(fiber_raw: str) -> str:
    """
    Trim marketing / garment text from the fiber phrase.
    Examples:
      'Organic Cotton – Your new favorite hoodie' -> 'Organic Cotton'
      'cotton harem pant that may remind you'     -> 'cotton'
    """
    s = fiber_raw.strip()

    # Cut at dash segments
    s = re.split(r"\s[-–—]\s", s)[0]

    # Cut at common sentence joiners
    for sep in [
        " with ",
        " that ",
        " which ",
        " featuring ",
        " crafted",
        " inspired",
        " perfect",
        " easily ",
    ]:
        lower = s.lower()
        idx = lower.find(sep)
        if idx != -1:
            s = s[:idx].strip()
            break

    # Cut when garment words start
    GARMENT_WORDS = [
        "hoodie",
        "jogger",
        "pant",
        "pants",
        "dress",
        "jumpsuit",
        "skirt",
        "shorts",
        "top",
        "tee",
        "t-shirt",
        "shirt",
        "sweater",
        "cardigan",
    ]
    lower = s.lower()
    cut_idx = None
    for gw in GARMENT_WORDS:
        idx = lower.find(" " + gw)
        if idx != -1:
            cut_idx = idx
            break
    if cut_idx is not None:
        s = s[:cut_idx].strip()

    # Remove trailing "solids", "prints", etc.
    s = re.sub(
        r"\b(solids?|prints?|pattern|exclusive of trim|shell|body)\b.*$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()

    return s.strip()


def parse_fabric_breakdown(materials: str):
    """
    Robust parser for Loomi Buddha Pants.

    Input:  'Wildflower/Marble: 80% cotton, 20% linenSolids: 100% cottonEasily worn...'
    Output: ('80% cotton, 20% linen, 100% cotton', ['COTTON', 'LINEN'])

    We:
      - normalize the text
      - find ALL 'NN% <phrase>' matches
      - discard matches with no fiber word
      - clean fiber phrases
      - aggregate by fiber to avoid weird duplicates
    """
    if not materials:
        return None, []

    text = materials.replace("\u00a0", " ")

    # Avoid weird concatenations like 'linenSolids'
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    text = text.replace("/", ",")

    # Start from first percentage if present (skip leading marketing copy)
    first_pct = re.search(r"\d{1,3}\s*%", text)
    if first_pct:
        text = text[first_pct.start():]

    # Find ALL "NN% some words" patterns up to comma/semicolon/colon/period/newline
    # Use word boundary \b to prevent capturing leading digits
    matches = re.findall(
        r"\b(\d{1,3})\s*%\s*([A-Za-z][^,.;:%\n]+)",
        text,
    )

    if not matches:
        return None, []

    # Aggregate by normalized fiber family
    fiber_totals = {}  # family -> total percent
    fiber_labels = {}  # family -> canonical label (first seen)
    seen_percentages = set()  # Track unique percentage+fiber combinations to avoid duplicates

    for pct_str, raw_phrase in matches:
        pct = int(pct_str)
        raw_phrase = raw_phrase.strip()

        # Skip things like "OFF EVERY" that do not contain fiber keywords
        if not is_fiber_phrase(raw_phrase):
            continue

        fiber_clean = clean_fiber_phrase(raw_phrase)
        if not fiber_clean or not is_fiber_phrase(fiber_clean):
            continue

        family = normalize_fiber_family(fiber_clean)
        
        # Create a unique key to avoid counting the same fiber+percent twice
        # (e.g., "80% cotton" appearing in both variant descriptions)
        unique_key = f"{pct}_{family}"
        if unique_key in seen_percentages:
            continue
        seen_percentages.add(unique_key)
        
        # Use max() instead of sum to handle cases where the same fiber 
        # appears multiple times with different percentages
        fiber_totals[family] = max(fiber_totals.get(family, 0), pct)
        
        # Remember the nicest label (first one)
        if family not in fiber_labels:
            fiber_labels[family] = fiber_clean

    if not fiber_totals:
        return None, []

    # Build sorted breakdown string from aggregated totals
    items = sorted(
        fiber_totals.items(),
        key=lambda kv: -kv[1],
    )

    breakdown_parts = []
    tags = []
    for family, pct in items:
        label = fiber_labels.get(family, family.title())
        breakdown_parts.append(f"{pct}% {label}")
        tags.append(family)

    breakdown = ", ".join(breakdown_parts)
    return breakdown, sorted(set(tags))
