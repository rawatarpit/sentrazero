#!/usr/bin/env python3
import sys
import json
import os
import re
import time
import logging
import random
import subprocess
import urllib.parse
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("scrape")

try:
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(json.dumps({"success": False, "error": f"Missing dependency: {e}"}))
    sys.exit(1)

try:
    import curl_cffi.requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

def ensure_playwright_ready() -> bool:
    if not HAS_PLAYWRIGHT:
        return False
    try:
        home = Path.home()
        cache_dir = home / ".cache" / "ms-playwright"
        if cache_dir.exists() and any(cache_dir.iterdir()):
            return True
        logger.info("Installing Playwright Chromium browser...")
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=True, capture_output=True, timeout=120
        )
        subprocess.run(
            [sys.executable, "-m", "playwright", "install-deps", "chromium"],
            check=True, capture_output=True, timeout=120
        )
        return True
    except Exception as e:
        logger.warning("Playwright setup incomplete: %s", e)
        return False


def init_playwright_browser():
    if not HAS_PLAYWRIGHT:
        return None, None
    try:
        p = sync_playwright().start()
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
        )
        return p, browser
    except Exception as e:
        logger.error("Failed to launch Playwright browser: %s", e)
        return None, None


DEFAULT_TIMEOUT = 15
BOT_INDICATORS = [
    "robot or human", "captcha", "verify you are human",
    "access denied", "please enable cookies", "blocked",
    "too many requests", "please try again later",
    "sorry, we were unable to complete your request",
    "automated access", "terms of service",
]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
]
STEALTH_JS = """
// Override webdriver
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
});

// Override plugins length
Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
});

// Override languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en'],
});

// Override chrome.runtime
window.chrome = {
    runtime: {},
    loadTimes: function() {},
    csi: function() {},
    app: {},
};

// Override permissions query
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({state: Notification.permission}) :
        originalQuery(parameters)
);
"""

MOCK_ATTRIBUTES = {
    "title": "Tai Pei Chicken Potstickers Frozen Asian Appetizers 46.5 oz",
    "brand": "Tai Pei",
    "upc": "078742123456",
    "gtin": "0078742123456",
    "ean": "",
    "model": "",
    "size": "46.5oz",
    "color": "",
    "price": 12.99,
    "pack_count": 1,
    "quantity": 1,
    "description": "Tai Pei Chicken Potstickers, 46.5 oz, Frozen Asian Appetizers",
    "images": [],
}

PLATFORM_SELECTORS: Dict[str, Dict[str, List[str]]] = {
    "walmart": {
        "title": ["h1", "[data-testid='product-title']"],
        "brand": ["a[data-testid='brand-name']", "span[itemprop='brand']"],
        "price": ["[itemprop='price']", "[data-testid='price']", ".price-group"],
        "description": ["[itemprop='description']", ".product-description"],
        "images": ["[data-testid='hero-image'] img", ".product-hero-image img"],
        "upc": ["[itemprop='gtin12']", "[itemprop='sku']", "[data-item-id]"],
        "gtin": ["[itemprop='gtin13']", "[itemprop='gtin14']"],
        "model": ["[itemprop='model']", "[data-model-id]"],
    },
    "amazon": {
        "title": ["#productTitle"],
        "brand": ["a#bylineInfo", "[itemprop='brand']"],
        "price": ["#priceblock_ourprice", "#corePrice_desktop .a-price-whole", "[itemprop='price']"],
        "description": ["#productDescription", "#feature-bullets"],
        "images": ["#landingImage", "#imgTagWrapperId img"],
        "upc": ["[itemprop='gtin12']", "[itemprop='sku']"],
        "gtin": ["[itemprop='gtin13']", "[itemprop='gtin14']"],
        "model": ["[itemprop='model']", "#product-subtitle"],
    },
}

# Comprehensive color word mapping (canonical → output color)
COLOR_MAP = {
    "alabaster": "white", "almond": "white", "apricot": "orange",
    "aqua": "blue", "aquamarine": "blue", "ash": "gray",
    "azure": "blue", "beige": "beige", "black": "black",
    "blue": "blue", "brown": "brown", "burgundy": "red",
    "camel": "brown", "cappuccino": "brown", "caramel": "brown",
    "cayenne": "red", "champagne": "white", "charcoal": "gray",
    "cherry": "red", "chestnut": "brown", "chili": "red",
    "chocolate": "brown", "cinnamon": "brown", "cobalt": "blue",
    "coffee": "brown", "coral": "orange", "cranberry": "red",
    "cream": "white", "crimson": "red", "cyan": "blue",
    "denim": "blue", "ebony": "black", "ecru": "white",
    "eggplant": "purple", "emerald": "green", "espresso": "brown",
    "fuchsia": "pink", "ginger": "brown", "gold": "gold",
    "graphite": "gray", "gray": "gray", "green": "green",
    "grey": "gray",     "gunmetal": "gray", "heather gray": "gray", "heather grey": "gray",
    "heather pink": "pink", "heather black": "black", "heather navy": "blue",
    "heather blue": "blue", "heather white": "white", "heather red": "red",
    "heather green": "green", "heather purple": "purple",
    "indigo": "purple", "ivory": "white", "jade": "green",
    "khaki": "khaki", "lavender": "purple", "lemon": "yellow",
    "lilac": "purple", "lime": "green", "magenta": "pink",
    "mahogany": "brown", "maroon": "red", "mauve": "purple",
    "midnight": "black", "mint": "green", "mocha": "brown",
    "mustard": "yellow", "navy": "blue", "oak": "brown",
    "olive": "green", "onyx": "black", "orange": "orange",
    "orchid": "purple", "peach": "pink", "pearl": "white",
    "periwinkle": "blue", "pink": "pink", "platinum": "gray",
    "plum": "purple", "purple": "purple", "raspberry": "red",
    "red": "red", "rose": "pink", "ruby": "red",
    "sage": "green", "salmon": "pink", "sapphire": "blue",
    "scarlet": "red", "silver": "gray", "skyblue": "blue",
    "slate": "gray", "smoke": "gray", "steelblue": "blue",
    "stone": "gray", "strawberry": "red", "tan": "brown",
    "tangerine": "orange", "taupe": "brown", "teal": "teal",
    "terracotta": "orange", "turquoise": "teal", "vanilla": "white",
    "violet": "purple", "walnut": "brown", "wheat": "white",
    "white": "white", "wine": "red", "yellow": "yellow",
}
# Common brand names / phrases that contain color words but aren't colors
COLOR_FALSE_POSITIVES = {
    "green sprouts", "brown sugar", "green giant",
    "red lobster", "blue buffalo", "black & decker",
    "black+decker", "black and decker", "white castle",
    "orange crush", "red bull", "green mountain",
    "black diamond", "brown university", "red pepper",
    "green bell pepper", "red bell pepper",
    "mt. olive", "mt olive",
}


def normalize_color(text: str, color_map_override: dict = None, color_false_positives_override: list = None) -> str:
    """Extract color from a product title using word-boundary matching.
    
    Strategy:
    1. Check parenthetical groups first (e.g., '(chocolate, 1 panel)')
    2. Word-boundary regex match against comprehensive color list
    3. Return empty string if no color found (never return the full title)
    
    color_map_override: additional color synonyms merged with built-in COLOR_MAP
    color_false_positives_override: additional false positive words merged with built-in COLOR_FALSE_POSITIVES
    """
    if not text:
        return ""
    lower = text.lower().strip()
    
    # Merge overrides with built-in constants
    if color_map_override is None:
        color_map_override = {}
    if color_false_positives_override is None:
        color_false_positives_override = []
    merged_color_map = {**COLOR_MAP, **{k.lower(): v for k, v in color_map_override.items()}}
    merged_false_positives = set(COLOR_FALSE_POSITIVES) | {fp.lower() for fp in color_false_positives_override}
    
    # Strategy 1: Check parenthetical groups (colors often in brackets)
    paren_groups = re.findall(r'[\(\[\{]([^\)\]\}]+)[\)\]\}]', lower)
    for group in paren_groups:
        for color, canonical in merged_color_map.items():
            if re.search(r'\b' + re.escape(color) + r'\b', group):
                return canonical
    
    # Remove false positive phrases before matching
    cleaned = lower
    for fp in merged_false_positives:
        cleaned = cleaned.replace(fp, " ")
    
    # Strategy 2: Word-boundary color matching by POSITION in text
    # Find the first color word that appears in the text
    best_pos = len(cleaned) + 1
    best_color = ""
    for color, canonical in merged_color_map.items():
        m = re.search(r'\b' + re.escape(color) + r'\b', cleaned)
        if m and m.start() < best_pos:
            best_pos = m.start()
            best_color = canonical
    
    if best_color:
        return best_color
    
    # No color found - return empty, not the full title
    return ""

SIZE_PATTERNS = [
    (re.compile(r"(\d+\.?\d*)\s*(ml|millilitre|milliliter)", re.I),
     lambda m: f"{float(m.group(1))/1000:.2f}L".replace(".00", "")),
    (re.compile(r"(\d+\.?\d*)\s*(oz|ounce)", re.I),
     lambda m: f"{m.group(1)}oz"),
    (re.compile(r"(\d+\.?\d*)\s*(lb|pound|lbs)", re.I),
     lambda m: f"{m.group(1)}lb"),
    (re.compile(r"(\d+\.?\d*)\s*(l|liter|litre)", re.I),
     lambda m: f"{m.group(1)}L"),
    (re.compile(r"(\d+\.?\d*)\s*(fl\.?\s*oz|fluid ounce)", re.I),
     lambda m: f"{m.group(1)}floz"),
    (re.compile(r"(\d+)\s*(count|ct|pack)", re.I),
     lambda m: f"{m.group(1)}ct"),
]


def safe_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    return str(val)


def load_job_input() -> Dict[str, Any]:
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON input: %s", e)
        print(json.dumps({"success": False, "error": f"Invalid JSON input: {e}"}))
        sys.exit(1)


def load_dataframe(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        print(json.dumps({"success": False, "error": f"Input not found: {path}"}))
        sys.exit(1)
    try:
        if p.suffix in (".xlsx", ".xls"):
            return pd.read_excel(path)
        return pd.read_csv(path, keep_default_na=False)
    except Exception as e:
        print(json.dumps({"success": False, "error": f"Failed to read input: {e}"}))
        sys.exit(1)


def output_result(result: Dict[str, Any]) -> None:
    print(json.dumps(result, ensure_ascii=False))
    sys.exit(0)


def extract_json_ld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    results = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                results.append(data)
            elif isinstance(data, list):
                results.extend(data)
        except (json.JSONDecodeError, TypeError):
            pass
    return results


def extract_from_json_ld(jsonld: List[Dict[str, Any]], platform: str) -> Dict[str, Any]:
    attrs: Dict[str, Any] = {}
    for item in jsonld:
        if not isinstance(item, dict):
            continue
        if item.get("@type") in ("Product", "ItemPage"):
            if "name" in item and not attrs.get("title"):
                attrs["title"] = item["name"]
            if "brand" in item:
                brand = item["brand"]
                if isinstance(brand, dict):
                    attrs["brand"] = brand.get("name", "")
                else:
                    attrs["brand"] = str(brand)
            if "description" in item and not attrs.get("description"):
                attrs["description"] = item["description"]
            if "sku" in item and not attrs.get("upc"):
                attrs["upc"] = item["sku"]
            if "mpn" in item and not attrs.get("model"):
                attrs["model"] = item["mpn"]
            if "image" in item:
                img = item["image"]
                if isinstance(img, str):
                    attrs.setdefault("images", []).append(img)
                elif isinstance(img, dict) and img.get("url"):
                    attrs.setdefault("images", []).append(img["url"])
            if "offers" in item:
                offers = item["offers"]
                if isinstance(offers, dict):
                    if "price" in offers and not attrs.get("price"):
                        attrs["price"] = float(offers["price"])
                    if "priceCurrency" in offers:
                        attrs["currency"] = offers["priceCurrency"]
                elif isinstance(offers, list) and offers:
                    if "price" in offers[0] and not attrs.get("price"):
                        attrs["price"] = float(offers[0]["price"])
            for gtin_key in ("gtin12", "gtin13", "gtin14", "gtin8"):
                if gtin_key in item:
                    val = str(item[gtin_key])
                    if gtin_key == "gtin12":
                        attrs["upc"] = val
                    else:
                        attrs["gtin"] = val
                    break
    return attrs


def extract_by_selectors(soup: BeautifulSoup, selectors: List[str], attr: str = "text") -> str:
    for sel in selectors:
        tag = soup.select_one(sel)
        if tag:
            if attr == "text":
                return tag.get_text(strip=True)
            elif attr == "src":
                return tag.get("src", "")
            elif attr == "href":
                return tag.get("href", "")
            elif attr == "content":
                return tag.get("content", "")
            elif attr == "data-item-id":
                return tag.get("data-item-id", "")
    return ""


def extract_images(soup: BeautifulSoup, selectors: List[str]) -> List[str]:
    urls = []
    for sel in selectors:
        tags = soup.select(sel)
        for tag in tags:
            src = tag.get("src") or tag.get("data-src") or ""
            if src and src.startswith("http"):
                urls.append(src)
        if urls:
            break
    return urls


def normalize_size(text: str) -> str:
    if not text:
        return ""
    for pattern, fmt in SIZE_PATTERNS:
        m = pattern.search(text)
        if m:
            return fmt(m)
    return ""


def _extract_specs_from_idml(initial_data: Optional[Dict]) -> Dict[str, str]:
    """Extract structured product specifications from Walmart's idml data.
    
    Walmart's idml can live at:
      - initialData.data.idml      (common for newer Walmart pages)
      - initialData.idml           (some older / variant pages)
    
    This function checks BOTH paths and returns the first found.
    Returns a dict with keys like 'color', 'size' when found.
    """
    result: Dict[str, str] = {}
    if not isinstance(initial_data, dict):
        return result
    
    # Try direct idml path first
    idml = initial_data.get("idml")
    
    # If not found, try nested under .data (most common for Walmart products)
    if not isinstance(idml, (dict, list)):
        data_obj = initial_data.get("data")
        if isinstance(data_obj, dict):
            idml = data_obj.get("idml")
    
    # idml varies: sometimes a list [{}], sometimes a dict directly
    idml_data = None
    if isinstance(idml, dict):
        idml_data = idml
    elif isinstance(idml, list) and len(idml) > 0 and isinstance(idml[0], dict):
        idml_data = idml[0]
    
    if not isinstance(idml_data, dict):
        return result
    
    # Map of spec name (lowercase) -> attribute key
    # Extended to capture ALL physical product specs from Walmart's structured data
    SPEC_MAP = {
        "color": "color",
        "clothing size": "size",
        "shoe size": "size",
        "size": "size",
        "weight": "weight",
        "product weight": "weight",
        "item weight": "weight",
        "total weight": "weight",
        "shipping weight": "weight",
        "volume": "volume",
        "product volume": "volume",
        "item volume": "volume",
        "capacity": "volume",
        "fluid volume": "volume",
        "material": "material",
        "product material": "material",
        "fabric": "material",
        "dimensions": "dimensions",
        "product dimensions": "dimensions",
        "item dimensions": "dimensions",
        "package dimensions": "dimensions",
        "count": "count",
        "pack count": "count",
        "quantity": "quantity",
        "number of items": "quantity",
        "model": "model",
        "model number": "model",
    }
    
    for spec_list_key in ("specifications", "productHighlights"):
        spec_list = idml_data.get(spec_list_key)
        if isinstance(spec_list, list):
            for spec in spec_list:
                if not isinstance(spec, dict):
                    continue
                name = str(spec.get("name", "")).strip().lower()
                value = str(spec.get("value", "")).strip()
                if not value:
                    continue
                attr_key = SPEC_MAP.get(name)
                if attr_key and attr_key not in result:
                    result[attr_key] = value
    
    # Try Walmart's nested productDescriptors structure:
    #   initialData.data.product.productDescriptors.specificationGroups[].specifications[]
    # Note: productDescriptors is at the product level, NOT inside idml
    product = initial_data.get("data", {}).get("product", {})
    if isinstance(product, dict):
        descriptors = product.get("productDescriptors")
        if isinstance(descriptors, dict):
            for sg in descriptors.get("specificationGroups", []):
                if not isinstance(sg, dict):
                    continue
                specs = sg.get("specifications", [])
                if not isinstance(specs, list):
                    continue
                for spec in specs:
                    if not isinstance(spec, dict):
                        continue
                    name = str(spec.get("name", "")).strip().lower()
                    value = str(spec.get("value", "")).strip()
                    if not value:
                        continue
                    attr_key = SPEC_MAP.get(name)
                    if attr_key and attr_key not in result:
                        result[attr_key] = value
    
    return result


NEXT_DATA_FIELDS = {
    "title": ("name", str),
    "brand": ("brand", str),
    "upc": ("upc", str),
    "gtin": ("gtin", str),
    "model": ("model", str),
    "description": ("shortDescription", str),
}


def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    try:
        data = json.loads(script.string)
    except (json.JSONDecodeError, TypeError):
        return None
    
    full_initial_data = (
        data.get("props", {})
        .get("pageProps", {})
        .get("initialData", {})
    )
    if not isinstance(full_initial_data, dict):
        return None
    
    # Product data is nested under .data in Walmart's initialData
    initial_data = full_initial_data.get("data", {})
    if not isinstance(initial_data, dict):
        return None
    
    product = initial_data.get("product")
    if not product or not isinstance(product, dict):
        return None
    
    attrs: Dict[str, Any] = {}
    for field, (key, val_type) in NEXT_DATA_FIELDS.items():
        val = product.get(key)
        if val and val_type is str:
            val = str(val).strip()
        attrs[field] = val if val else ""
    
    price_info = product.get("priceInfo", {})
    if isinstance(price_info, dict):
        cp = price_info.get("currentPrice", {})
        if isinstance(cp, dict):
            attrs["price"] = float(cp.get("price", 0) or 0)
        else:
            attrs["price"] = 0.0
    else:
        attrs["price"] = 0.0
    
    images = []
    img_info = product.get("imageInfo", {})
    if isinstance(img_info, dict):
        all_imgs = img_info.get("allImages", [])
        if isinstance(all_imgs, list):
            for img in all_imgs:
                if isinstance(img, dict) and img.get("url"):
                    images.append(img["url"])
    attrs["images"] = images[:5]
    
    # ---- Extract structured specifications from Walmart's idml data ----
    # _extract_specs_from_idml checks both initialData.data.idml and
    # initialData.idml, so we pass the full initialData object.
    specs = _extract_specs_from_idml(full_initial_data)
    
    # ---- Extract ALL specs from productDescriptors (weight, volume, etc.) ----
    # This captures specs the SPEC_MAP might miss
    all_product_specs: Dict[str, str] = {}
    product_descriptors = product.get("productDescriptors")
    if isinstance(product_descriptors, dict):
        for sg in product_descriptors.get("specificationGroups", []):
            if not isinstance(sg, dict):
                continue
            for spec in sg.get("specifications", []):
                if not isinstance(spec, dict):
                    continue
                name = str(spec.get("name", "")).strip()
                value = str(spec.get("value", "")).strip()
                if name and value:
                    all_product_specs[name.lower()] = value
    
    # Also check idml specs
    idml_obj = full_initial_data.get("data", {}).get("idml")
    if not isinstance(idml_obj, (dict, list)):
        idml_obj = full_initial_data.get("idml")
    if isinstance(idml_obj, dict):
        for spec_list_key in ("specifications", "productHighlights"):
            spec_list = idml_obj.get(spec_list_key)
            if isinstance(spec_list, list):
                for spec in spec_list:
                    if isinstance(spec, dict):
                        name = str(spec.get("name", "")).strip().lower()
                        value = str(spec.get("value", "")).strip()
                        if name and value and name not in all_product_specs:
                            all_product_specs[name] = value
    
    # Store all specs in attrs for downstream use (compare.py can read these)
    attrs["all_specs"] = all_product_specs
    
    # Check zeekitData for a more specific color (e.g. "Retro Heather Pink" vs "Pink")
    zeekit_color = None
    zeekit = product.get("zeekitData")
    if isinstance(zeekit, dict):
        zc = zeekit.get("color")
        if zc and isinstance(zc, str) and zc.strip():
            zeekit_color = zc.strip()
    
    # Priority for color: zeekit > specs > title-based guess
    if zeekit_color:
        attrs["color"] = zeekit_color
    elif specs.get("color"):
        attrs["color"] = specs["color"]
    else:
        attrs["color"] = normalize_color(attrs.get("title", ""))
    
    # Priority for size: specs > title-based guess
    if specs.get("size"):
        attrs["size"] = specs["size"]
    else:
        size = normalize_size(attrs.get("title", ""))
        if not size:
            size = normalize_size(attrs.get("description", ""))
        attrs["size"] = size
    
    pack_count = 1
    ct_match = re.search(r"(\d+)\s*(count|ct|pack)\b", attrs.get("title", ""), re.I)
    if ct_match:
        pack_count = int(ct_match.group(1))
    attrs["pack_count"] = pack_count
    attrs["quantity"] = 1
    return attrs


def detect_bot_block(html: str) -> Optional[str]:
    text = html.lower()[:2000]
    for indicator in BOT_INDICATORS:
        if indicator in text:
            return indicator
    return None


def fetch_with_playwright(url: str, platform: str, timeout: int, browser=None,
                           proxy_url: str = "",
                           use_stealth_js: bool = True,
                           extract_from_idml: bool = True,
                           extract_from_jsonld: bool = True,
                           color_map_override: dict = None,
                           color_false_positives_override: list = None) -> Dict[str, Any]:
    if not HAS_PLAYWRIGHT or browser is None:
        return {"success": False, "url": url, "error": "Playwright not available"}
    try:
        ua = random.choice(USER_AGENTS)
        width = random.choice([1920, 1366, 1536, 1440])
        height = random.choice([1080, 768, 864, 900])
        ctx_kwargs: Dict[str, Any] = {
            "user_agent": ua,
            "viewport": {"width": width, "height": height},
            "device_scale_factor": random.choice([1, 2]),
        }
        if proxy_url:
            ctx_kwargs["proxy"] = {"server": proxy_url}
        ctx_kwargs.update({
            "is_mobile": False,
            "has_touch": False,
            "locale": "en-US",
            "timezone_id": random.choice(["America/New_York", "America/Chicago", "America/Los_Angeles", "America/Denver"]),
            "extra_http_headers": {
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
        })
        ctx = browser.new_context(**ctx_kwargs)
        page = ctx.new_page()
        if use_stealth_js:
            page.add_init_script(STEALTH_JS)
        page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
        time.sleep(random.uniform(1.0, 3.0))
        html = page.content()
        ctx.close()
        bot_reason = detect_bot_block(html)
        if bot_reason:
            logger.warning("Playwright bot block on %s: '%s'", url, bot_reason)
            return {"success": False, "url": url, "error": f"Bot blocked (Playwright): {bot_reason}"}
        attrs = extract_attributes(html, platform, extract_from_idml=extract_from_idml, extract_from_jsonld=extract_from_jsonld,
                                    color_map_override=color_map_override, color_false_positives_override=color_false_positives_override)
        if attrs.get("title"):
            logger.info("Playwright scraped %s successfully: %s", url, attrs.get("title", "")[:60])
        else:
            logger.warning("Playwright loaded %s but extracted no title", url)
        return {"success": True, "url": url, "attributes": attrs}
    except Exception as e:
        logger.error("Playwright failed for %s: %s", url, e)
        return {"success": False, "url": url, "error": f"Playwright: {e}"}


def fetch_with_curl_cffi(url: str, platform: str, timeout: int,
                          impersonate: str = "chrome",
                          proxy_url: str = "",
                          extract_from_idml: bool = True,
                          extract_from_jsonld: bool = True,
                          color_map_override: dict = None,
                          color_false_positives_override: list = None) -> Dict[str, Any]:
    if not HAS_CURL_CFFI:
        return {"success": False, "url": url, "error": "curl_cffi not available"}
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "DNT": "1",
    }
    try:
        kwargs: Dict[str, Any] = {
            "impersonate": impersonate,
            "headers": headers,
            "timeout": timeout,
        }
        if proxy_url:
            kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
        r = curl_requests.get(url, **kwargs)
        if r.status_code != 200:
            logger.warning("curl_cffi HTTP %d for %s", r.status_code, url)
            return {"success": False, "url": url, "error": f"HTTP {r.status_code}"}
        bot_reason = detect_bot_block(r.text)
        if bot_reason:
            logger.warning("curl_cffi bot block on %s: '%s'", url, bot_reason)
            return {"success": False, "url": url, "error": f"Bot blocked: {bot_reason}"}
        next_attrs = extract_next_data(r.text)
        if next_attrs and next_attrs.get("title"):
            logger.info("curl_cffi next_data %s: %s", url, next_attrs["title"][:60])
            return {"success": True, "url": url, "attributes": next_attrs}
        attrs = extract_attributes(r.text, platform, extract_from_idml=extract_from_idml, extract_from_jsonld=extract_from_jsonld,
                                    color_map_override=color_map_override, color_false_positives_override=color_false_positives_override)
        if attrs.get("title"):
            logger.info("curl_cffi scraped %s: %s", url, attrs["title"][:60])
        else:
            logger.warning("curl_cffi loaded %s but no data extracted", url)
        return {"success": True, "url": url, "attributes": attrs}
    except Exception as e:
        logger.error("curl_cffi failed for %s: %s", url, e)
        return {"success": False, "url": url, "error": f"curl_cffi: {e}"}


def fetch_url(url: str, platform: str, mock: bool, user_agent: str,
              timeout: int, mock_attrs: Dict[str, Any], browser=None,
              proxy_url: str = "", impersonate: str = "chrome",
              use_curl_cffi: bool = True,
              fallback_to_playwright: bool = True,
              retry_count: int = 2,
              retry_delay_seconds: int = 1,
              use_stealth_js: bool = True,
              extract_from_idml: bool = True,
              extract_from_jsonld: bool = True,
              color_map_override: dict = None,
              color_false_positives_override: list = None,
              description_max_length: int = 1000,
              image_limit: int = 5) -> Dict[str, Any]:
    if mock:
        logger.info("Mock mode for %s", url)
        return {"success": True, "url": url, "attributes": dict(mock_attrs)}
    best_error = ""
    # Try curl_cffi first if enabled (handles TLS fingerprinting for Walmart)
    if use_curl_cffi and HAS_CURL_CFFI and ("walmart.com" in url or "amazon.com" in url or "costco.com" in url):
        result = fetch_with_curl_cffi(url, platform, timeout, impersonate, proxy_url,
                                       extract_from_idml=extract_from_idml,
                                       extract_from_jsonld=extract_from_jsonld,
                                       color_map_override=color_map_override,
                                       color_false_positives_override=color_false_positives_override)
        if result.get("success"):
            return result
        best_error = result.get("error", "")
        logger.info("curl_cffi failed for %s (%s), trying fallback", url, best_error)
    ua_list = USER_AGENTS[:retry_count] if retry_count > 0 else USER_AGENTS[:1]
    if user_agent:
        ua_list = [user_agent] + [ua for ua in USER_AGENTS[:retry_count] if ua != user_agent]
    ua_list = ua_list[:max(1, retry_count)]
    last_error = ""
    for attempt, ua in enumerate(ua_list):
        try:
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": ua,
                "Referer": "https://www.google.com/",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            kwargs: Dict[str, Any] = {"headers": headers, "timeout": timeout}
            if proxy_url:
                kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
            resp = requests.get(url, **kwargs)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s (attempt %d)", resp.status_code, url, attempt + 1)
                last_error = f"HTTP {resp.status_code}"
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
                continue
            bot_reason = detect_bot_block(resp.text)
            if bot_reason:
                logger.warning("Bot block detected on %s: '%s'", url, bot_reason)
                last_error = f"Bot blocked: {bot_reason}"
                if retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)
                continue
            attrs = extract_attributes(resp.text, platform, extract_from_idml=extract_from_idml,
                                        extract_from_jsonld=extract_from_jsonld,
                                        color_map_override=color_map_override,
                                        color_false_positives_override=color_false_positives_override,
                                        description_max_length=description_max_length,
                                        image_limit=image_limit)
            return {"success": True, "url": url, "attributes": attrs}
        except requests.RequestException as e:
            logger.error("Request failed for %s (attempt %d): %s", url, attempt + 1, e)
            last_error = str(e)
            if retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)
            continue
    # All requests attempts failed or were blocked — try Playwright as fallback
    if fallback_to_playwright:
        if not best_error and last_error:
            best_error = last_error
        logger.info("Requests failed for %s, trying Playwright fallback (best prior error: %s)", url, best_error)
        pw_result = fetch_with_playwright(url, platform, timeout, browser=browser, proxy_url=proxy_url,
                                           use_stealth_js=use_stealth_js,
                                           extract_from_idml=extract_from_idml,
                                           extract_from_jsonld=extract_from_jsonld,
                                           color_map_override=color_map_override,
                                           color_false_positives_override=color_false_positives_override)
        if not pw_result.get("success") and best_error:
            pw_error = pw_result.get("error", "")
            pw_result["error"] = best_error
            pw_result["_fallback_note"] = f"Playwright also failed: {pw_error}" if pw_error else ""
        return pw_result
    
    # No fallback - return the best error we have
    return {"success": False, "url": url, "error": best_error or last_error or "All fetch attempts failed"}


def extract_attributes(html: str, platform: str,
                        extract_from_idml: bool = True,
                        extract_from_jsonld: bool = True,
                        color_map_override: dict = None,
                        color_false_positives_override: list = None,
                        description_max_length: int = 1000,
                        image_limit: int = 5) -> Dict[str, Any]:
    if color_map_override is None:
        color_map_override = {}
    if color_false_positives_override is None:
        color_false_positives_override = []
    soup = BeautifulSoup(html, "lxml") if "lxml" in str(type) else BeautifulSoup(html, "html.parser")
    selectors = PLATFORM_SELECTORS.get(platform, PLATFORM_SELECTORS.get("walmart", {}))
    jsonld = extract_json_ld(soup)
    ld_attrs = extract_from_json_ld(jsonld, platform) if extract_from_jsonld else {}
    title = extract_by_selectors(soup, selectors.get("title", ["h1"])) or ld_attrs.get("title", "")
    brand = extract_by_selectors(soup, selectors.get("brand", []))
    if not brand:
        brand = ld_attrs.get("brand", "")
    brand = brand.replace("Brand:", "").replace("by ", "").strip()
    upc = ld_attrs.get("upc", "")
    if not upc:
        upc = extract_by_selectors(soup, selectors.get("upc", []), attr="data-item-id")
    gtin = ld_attrs.get("gtin", "")
    if not gtin:
        gtin = extract_by_selectors(soup, selectors.get("gtin", []), attr="content")
    model = ld_attrs.get("model", "")
    if not model:
        model = extract_by_selectors(soup, selectors.get("model", []))
    price_str = extract_by_selectors(soup, selectors.get("price", []))
    price = ld_attrs.get("price", 0.0)
    if not price and price_str:
        try:
            price = float(re.sub(r"[^0-9.]", "", price_str))
        except (ValueError, TypeError):
            price = 0.0
    description = ld_attrs.get("description", "")
    if not description:
        description = extract_by_selectors(soup, selectors.get("description", []))
    images = ld_attrs.get("images", [])
    if not images:
        images = extract_images(soup, selectors.get("images", []))
    
    # ---- Try to enrich color/size from __NEXT_DATA__ structured specs ----
    next_specs: Dict[str, str] = {}
    all_specs: Dict[str, str] = {}
    if platform == "walmart" and extract_from_idml:
        next_data = extract_next_data(html)
        if next_data:
            # Use the enriched color/size from __NEXT_DATA__ which has idml specs
            if next_data.get("color") and normalize_color(title, color_map_override, color_false_positives_override) == normalize_color(title, color_map_override, color_false_positives_override).lower():
                pass
            for key in ("color", "size"):
                if next_data.get(key) and (key == "size" or not normalize_color(title, color_map_override, color_false_positives_override) or next_data[key] != normalize_color(title, color_map_override, color_false_positives_override)):
                    next_specs[key] = next_data[key]
            # Capture all product specs (weight, volume, dimensions, etc.)
            all_specs = next_data.get("all_specs", {})
    
    size = normalize_size(title)
    if not size:
        size = normalize_size(description)
    color = normalize_color(title, color_map_override, color_false_positives_override)
    
    # Override with __NEXT_DATA__ enriched values if available
    if next_specs.get("size"):
        size = next_specs["size"]
    if next_specs.get("color"):
        color = next_specs["color"]
    
    # ---- Extract weight/volume from specs or description ----
    weight = ""
    volume = ""
    # Try to find weight from all_specs
    for spec_key, spec_val in all_specs.items():
        if "weight" in spec_key and spec_val:
            weight = spec_val
        if any(vk in spec_key for vk in ("volume", "capacity", "fluid")) and spec_val:
            volume = spec_val
    # Fallback: extract weight from title/description
    if not weight:
        weight_patterns = [
            re.compile(r"(\d+\.?\d*)\s*(?:lb|pound|lbs)\b", re.I),
            re.compile(r"(\d+\.?\d*)\s*(?:oz|ounce)\b", re.I),
            re.compile(r"(\d+\.?\d*)\s*(?:kg|kilogram)\b", re.I),
            re.compile(r"(\d+\.?\d*)\s*(?:g|gram|grams)\b", re.I),
        ]
        for pat in weight_patterns:
            m = pat.search(title)
            if not m:
                m = pat.search(description)
            if m:
                weight = m.group(0).strip()
                break
    # Fallback: extract volume from title/description
    if not volume:
        vol_patterns = [
            re.compile(r"(\d+\.?\d*)\s*(?:fl\.?\s*oz|fluid\s*ounce)\b", re.I),
            re.compile(r"(\d+\.?\d*)\s*(?:ml|milliliter|millilitre)\b", re.I),
            re.compile(r"(\d+\.?\d*)\s*(?:l|liter|litre)\b", re.I),
        ]
        for pat in vol_patterns:
            m = pat.search(title)
            if not m:
                m = pat.search(description)
            if m:
                volume = m.group(0).strip()
                break
    
    pack_count = 1
    ct_match = re.search(r"(\d+)\s*(count|ct|pack)\b", title, re.I)
    if ct_match:
        pack_count = int(ct_match.group(1))
    return {
        "title": title,
        "brand": brand,
        "upc": upc,
        "gtin": gtin,
        "model": model,
        "size": size,
        "color": color,
        "weight": weight,
        "volume": volume,
        "price": price,
        "pack_count": pack_count,
        "quantity": 1,
        "description": description[:description_max_length] if description and description_max_length > 0 else (description or ""),
        "images": images[:image_limit] if image_limit > 0 else images,
    }


def _resolve_payload(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize plugin input to support both runtime formats.

    v2 runtime:   {"payload": {"input_path": "...", "config": {...}}}
    native:       {"input_path": "...", "config": {...}}

    Returns a flat dict with all fields + config merged.
    """
    payload = input_data.get("payload")
    if payload is None:
        payload = dict(input_data)  # native: fields at top level
    # Merge step config so config fields are directly accessible
    cfg = payload.get("config", {})
    if isinstance(cfg, dict):
        for k, v in cfg.items():
            if k not in payload:
                payload[k] = v
    return payload


def main() -> None:
    try:
        input_data = load_job_input()
        payload = _resolve_payload(input_data)
        input_path = payload.get("input_path") or payload.get("source_path")
        if not input_path:
            input_keys = sorted(input_data.keys())
            payload_keys = sorted(payload.keys())
            output_result({"success": False, "error": f"Missing input_path or source_path. input_data keys={input_keys}, payload keys={payload_keys}"})
        user_agent = payload.get("user_agent", "")
        timeout = int(payload.get("request_timeout", DEFAULT_TIMEOUT))
        mock = bool(payload.get("mock_mode", os.environ.get("RISEOTB_MOCK_MODE", "false").lower() == "true"))
        mock_attrs = payload.get("mock_attributes", MOCK_ATTRIBUTES)
        url_columns = payload.get("url_columns", ["url"])
        platforms_map = payload.get("platforms", {})
        output_path = payload.get("output_path", "")
        proxy_url = payload.get("proxy_url", "")
        impersonate = payload.get("impersonate", "chrome")
        # --- New configurable fields from config_schema ---
        retry_count = int(payload.get("retry_count", 2))
        retry_delay_seconds = int(payload.get("retry_delay_seconds", 1))
        fallback_to_playwright = bool(payload.get("fallback_to_playwright", True))
        use_curl_cffi = bool(payload.get("use_curl_cffi", True))
        use_stealth_js = bool(payload.get("use_stealth_js", True))
        extract_from_idml = bool(payload.get("extract_from_idml", True))
        extract_from_jsonld = bool(payload.get("extract_from_jsonld", True))
        raw_color_map = payload.get("color_map", {})
        if isinstance(raw_color_map, str):
            try:
                color_map_override = json.loads(raw_color_map) if raw_color_map and raw_color_map.strip() != "{}" else {}
            except (json.JSONDecodeError, TypeError):
                color_map_override = {}
        else:
            color_map_override = raw_color_map or {}
        raw_false_positives = payload.get("color_false_positives", [])
        if isinstance(raw_false_positives, str):
            try:
                color_false_positives_override = json.loads(raw_false_positives) if raw_false_positives and raw_false_positives.strip() != "[]" else []
            except (json.JSONDecodeError, TypeError):
                color_false_positives_override = []
        else:
            color_false_positives_override = raw_false_positives or []
        image_limit = int(payload.get("image_limit", 5))
        description_max_length = int(payload.get("description_max_length", 1000))
        if not output_path:
            stem = Path(input_path).stem
            output_path = str(Path(input_path).parent / f"{stem}_scraped.csv")
        logger.info("Loading %s", input_path)
        df = load_dataframe(input_path)
        logger.info("Loaded %d rows", len(df))

        # Initialize shared Playwright browser
        pw_available = ensure_playwright_ready()
        pw_instance, browser = init_playwright_browser() if pw_available else (None, None)
        if browser:
            logger.info("Playwright browser ready for fallback scraping")

        results = []
        success_count = 0
        for _, row in df.iterrows():
            entry = row.to_dict()
            for col in url_columns:
                url = safe_str(row.get(col, ""))
                platform = platforms_map.get(col, "generic")
                prefix = col
                if not url.startswith("http"):
                    entry[f"{col}_success"] = False
                    entry[f"{col}_attributes_json"] = json.dumps({"_scrape_error": "Invalid or empty URL"}, ensure_ascii=False)
                    for attr in ["title", "brand", "upc", "gtin", "model", "size", "color", "weight", "volume", "price", "pack_count", "quantity", "description"]:
                        entry[f"{prefix}_{attr}"] = ""
                    entry[f"{prefix}_images_json"] = "[]"
                else:
                    result = fetch_url(url, platform, mock, user_agent, timeout, mock_attrs, browser=browser,
                                        proxy_url=proxy_url, impersonate=impersonate,
                                        use_curl_cffi=use_curl_cffi, fallback_to_playwright=fallback_to_playwright,
                                        retry_count=retry_count, retry_delay_seconds=retry_delay_seconds,
                                        use_stealth_js=use_stealth_js,
                                        extract_from_idml=extract_from_idml, extract_from_jsonld=extract_from_jsonld,
                                        color_map_override=color_map_override,
                                        color_false_positives_override=color_false_positives_override,
                                        description_max_length=description_max_length, image_limit=image_limit)
                    is_ok = result.get("success", False)
                    entry[f"{col}_success"] = is_ok
                    if is_ok:
                        success_count += 1
                        attrs = result.get("attributes", {})
                    else:
                        attrs = {"_scrape_error": result.get("error", "Unknown error")}
                    entry[f"{col}_attributes_json"] = json.dumps(attrs, ensure_ascii=False)
                    entry[f"{prefix}_title"] = attrs.get("title", "")
                    entry[f"{prefix}_brand"] = attrs.get("brand", "")
                    entry[f"{prefix}_upc"] = attrs.get("upc", "")
                    entry[f"{prefix}_gtin"] = attrs.get("gtin", "")
                    entry[f"{prefix}_model"] = attrs.get("model", "")
                    entry[f"{prefix}_size"] = attrs.get("size", "")
                    entry[f"{prefix}_color"] = attrs.get("color", "")
                    entry[f"{prefix}_weight"] = attrs.get("weight", "")
                    entry[f"{prefix}_volume"] = attrs.get("volume", "")
                    entry[f"{prefix}_price"] = attrs.get("price", 0.0)
                    entry[f"{prefix}_pack_count"] = attrs.get("pack_count", 1)
                    entry[f"{prefix}_quantity"] = attrs.get("quantity", 1)
                    entry[f"{prefix}_description"] = attrs.get("description", "")
                    entry[f"{prefix}_images_json"] = json.dumps(attrs.get("images", []), ensure_ascii=False)
            results.append(entry)
        # Cleanup Playwright browser
        if browser:
            try:
                browser.close()
            except Exception:
                pass
        if pw_instance:
            try:
                pw_instance.stop()
            except Exception:
                pass

        result_df = pd.DataFrame(results)
        result_df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info("Wrote %d rows to %s", len(results), output_path)
        created_columns = {col: f"{col}_attributes_json" for col in url_columns}
        output_result({
            "success": True,
            "data": {
                "processed_rows": len(results),
                "output_path": output_path,
                "metadata": {
                    "mode": "scrape_urls",
                    "url_columns": url_columns,
                    "success_count": success_count,
                    "created_columns": created_columns,
                },
            },
            "error": None,
        })
    except Exception as e:
        logger.exception("Unhandled error in main()")
        print(json.dumps({"success": False, "error": f"Internal plugin error: {e}"}, ensure_ascii=False))
        sys.exit(1)

if __name__ == "__main__":
    main()
