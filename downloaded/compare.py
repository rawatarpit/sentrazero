#!/usr/bin/env python3
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("compare")

try:
    import pandas as pd
    from rapidfuzz import fuzz
except ImportError:
    from difflib import SequenceMatcher
    class _Fuzz:
        @staticmethod
        def token_sort_ratio(s1: str, s2: str) -> float:
            a = " ".join(sorted(s1.split()))
            b = " ".join(sorted(s2.split()))
            return SequenceMatcher(None, a, b).ratio() * 100
        @staticmethod
        def ratio(s1: str, s2: str) -> float:
            return SequenceMatcher(None, s1, s2).ratio() * 100
    fuzz = _Fuzz()

WEIGHTS = {
    "upc": 0.50,
    "brand": 0.20,
    "title": 0.15,
    "size": 0.10,
    "color": 0.05,
}

# Color keywords used by is_likely_color
COLOR_KEYWORDS = {
    "red", "blue", "green", "black", "white", "pink", "yellow", "orange",
    "purple", "brown", "gray", "grey", "gold", "silver", "navy", "teal",
    "coral", "beige", "tan", "cream", "ivory", "maroon", "burgundy",
    "lavender", "lilac", "violet", "indigo", "turquoise", "aqua", "mint",
    "peach", "salmon", "rose", "blush", "champagne", "bronze", "copper",
    "rust", "olive", "khaki", "charcoal", "magenta", "cyan", "lime",
    "crimson", "ruby", "emerald", "sapphire", "amethyst", "jade",
    "plum", "wine", "berry", "apricot", "mocha", "cocoa", "taupe",
    "mauve", "fuchsia", "hot pink", "neon", "fluorescent",
    "multicolor", "multi", "assorted", "rainbow",
    "clear", "transparent", "opaque", "metallic", "iridescent",
    "light", "dark", "medium", "bright", "pale", "deep",
    "heather", "retro heather", "classic", "vintage", "washed",
}

# Client output format helpers
def extract_walmart_id(url: str) -> str:
    m = re.search(r'/ip/(\d+)', url)
    return m.group(1) if m else url


def extract_amazon_asin(url: str) -> str:
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else url


def format_walmart_url(url: str) -> str:
    if '?' in url:
        return url
    return url + '?selected=true'


def format_comp_url(url: str) -> str:
    if '?' in url:
        return url
    return url + '?psc=1&th=1&vs=1'


COLOR_NORMALIZATION = {
    "navy blue": "blue", "navy": "blue", "midnight black": "black",
    "midnight": "black", "rose gold": "gold", "off white": "white",
    "cream": "white", "ivory": "white", "charcoal": "gray", "grey": "gray",
}


def load_job_input() -> Dict[str, Any]:
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(json.dumps({"success": False, "error": f"Invalid JSON input: {e}"}))
        sys.exit(1)


def load_dataframe(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        print(json.dumps({"success": False, "error": f"File not found: {path}"}))
        sys.exit(1)
    # Chunk outputs may carry a generic extension (e.g. .bin/.out) even when
    # the content is delimited text. Try CSV first, then fall back to Excel.
    try:
        return pd.read_csv(path)
    except Exception:
        pass
    try:
        return pd.read_excel(path)
    except Exception as e:
        print(json.dumps({"success": False, "error": f"Failed to read input: {e}"}))
        sys.exit(1)


def output_result(result: Dict[str, Any]) -> None:
    print(json.dumps(result))
    sys.exit(0)


def clean(text: Any) -> str:
    return " ".join(str(text).lower().strip().split())


BRAND_PATTERNS = [
    (re.compile(r"^visit the\s+(.+?)\s+store$", re.I), 1),
    (re.compile(r"^visit the\s+(.+)$", re.I), 1),
]


def normalize_brand(text: str) -> str:
    """Strip 'Visit the ... Store' / 'Visit the ...' wrappers from brand names."""
    if not text:
        return text
    lower = clean(text)
    for pat, group_idx in BRAND_PATTERNS:
        m = pat.match(lower)
        if m:
            return m.group(group_idx).strip()
    return lower


def normalize_color_val(text: str) -> str:
    lower = clean(text)
    for key, val in COLOR_NORMALIZATION.items():
        if key in lower:
            return val
    return lower


def normalize_size_val(text: str) -> str:
    if not text:
        return ""
    text = clean(text)
    patterns = [
        (re.compile(r"(\d+\.?\d*)\s*(ml|millilitre|milliliter)", re.I),
         lambda m: f"{float(m.group(1))/1000:.2f}L".replace(".00", "")),
        (re.compile(r"(\d+\.?\d*)\s*(oz|ounce)", re.I),
         lambda m: f"{m.group(1)}oz"),
        (re.compile(r"(\d+\.?\d*)\s*(lb|pound|lbs)", re.I),
         lambda m: f"{m.group(1)}lb"),
        (re.compile(r"(\d+\.?\d*)\s*(l|liter|litre)", re.I),
         lambda m: f"{m.group(1)}L"),
        (re.compile(r"(\d+)\s*(count|ct|pack)s?\b", re.I),
         lambda m: f"{m.group(1)}ct"),
        # Short abbreviations for "pack" — use \b to avoid false matches
        (re.compile(r"(\d+)\s*(p|pk|pkt)\b", re.I),
         lambda m: f"{m.group(1)}ct"),
    ]
    for pat, fmt in patterns:
        m = pat.search(text)
        if m:
            return fmt(m)
    return text


def is_likely_color(text: str) -> bool:
    """Check if text looks like a color name/description."""
    if not text:
        return False
    lower = text.lower().strip()
    # Short strings (<=15 chars) that contain known color words
    if len(lower) <= 15:
        for kw in COLOR_KEYWORDS:
            if kw in lower:
                return True
        return False
    # Longer strings: only return True if they look like a real color phrase
    # (contains known color words AND doesn't read like a product description)
    color_word_count = sum(1 for kw in COLOR_KEYWORDS if kw in lower)
    if color_word_count >= 2:
        return True
    # Check if it's a known multi-word color name
    multi_word_colors = ["retro heather", "classic", "heather", "vintage", "washed"]
    for mwc in multi_word_colors:
        if mwc in lower:
            return True
    return False


def parse_attributes(attrs_json: str) -> Dict[str, Any]:
    try:
        attrs = json.loads(attrs_json) if attrs_json and attrs_json != "{}" else {}
        if isinstance(attrs, dict):
            return {
                "title": str(attrs.get("title", "")),
                "brand": str(attrs.get("brand", "")),
                "upc": str(attrs.get("upc", "")),
                "gtin": str(attrs.get("gtin", "")),
                "size": str(attrs.get("size", "")),
                "color": str(attrs.get("color", "")),
                "weight": str(attrs.get("weight", "")),
                "volume": str(attrs.get("volume", "")),
                "model": str(attrs.get("model", "")),
                "price": float(attrs.get("price", 0.0)),
            }
    except (json.JSONDecodeError, TypeError):
        pass
    return {}


def compare_attributes(attr1: Dict[str, Any], attr2: Dict[str, Any],
                       config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if config is None:
        config = {}
    color_empty_means_match = config.get("color_empty_means_match", False)
    enable_title_only_match = config.get("enable_title_only_match", False)
    enable_upc_match_bypass = config.get("enable_upc_match_bypass", False)
    brand_similarity_threshold = config.get("brand_similarity_threshold", 0.8)
    title_similarity_threshold = config.get("title_similarity_threshold", 0.8)
    exact_match_threshold = config.get("exact_match_threshold", 0.6)

    upc1 = clean(attr1.get("upc", ""))
    upc2 = clean(attr2.get("upc", ""))
    brand1 = normalize_brand(attr1.get("brand", ""))
    brand2 = normalize_brand(attr2.get("brand", ""))
    title1 = attr1.get("title", "")
    title2 = attr2.get("title", "")
    size1 = normalize_size_val(attr1.get("size", ""))
    size2 = normalize_size_val(attr2.get("size", ""))
    color1 = normalize_color_val(attr1.get("color", ""))
    color2 = normalize_color_val(attr2.get("color", ""))
    confidence = 0.0
    reasons: List[str] = []
    reason_codes: List[str] = []
    notes: List[str] = []
    upc_match = bool(upc1 and upc2 and upc1 == upc2)
    if upc_match:
        confidence += WEIGHTS["upc"]
        reasons.append("UPC matched")
        reason_codes.append("UPC_MATCH")
        # UPC match bypass: if enabled and UPC matches, call it exact
        if enable_upc_match_bypass:
            return {
                "decision": "EXACT_MATCH",
                "exact_match": "Yes",
                "reason_code": "UPC_MATCH",
                "reason": "UPC matched",
                "confidence": 100.0,
                "notes": "",
            }
    # Brand comparison — use exact match (or fuzzy if brand_similarity_threshold < 1.0)
    brand_match = bool(brand1 and brand2 and brand1 == brand2)
    if not brand_match and brand1 and brand2:
        # Try fuzzy brand match when configured threshold < 1.0
        if brand_similarity_threshold < 1.0:
            try:
                brand_sim = fuzz.token_sort_ratio(brand1, brand2) / 100.0
                brand_match = brand_sim >= brand_similarity_threshold
            except Exception:
                pass
    if brand_match:
        confidence += WEIGHTS["brand"]
        reasons.append("Brand matched")
        reason_codes.append("BRAND_MATCH")
    # Note: brand differences are NOT included in notes — only physical attributes (Color, Size)
    title_sim = 0.0
    if title1 and title2:
        try:
            title_sim = fuzz.token_sort_ratio(title1, title2) / 100.0
        except Exception:
            title_sim = fuzz.ratio(title1, title2) / 100.0
        confidence += WEIGHTS["title"] * title_sim
        if title_sim >= title_similarity_threshold:
            reasons.append(f"Title similarity {title_sim:.0%}")
            reason_codes.append("TITLE_SIMILAR")
    size_match = bool(size1 and size2 and size1 == size2)
    if size_match:
        confidence += WEIGHTS["size"]
        reasons.append("Size matched")
        reason_codes.append("SIZE_MATCH")
    elif size1 and size2 and size1 != size2:
        notes.append("Size")
    color_match = bool(color1 and color2 and color1 == color2)
    if color_match:
        confidence += WEIGHTS["color"]
        reasons.append("Color matched")
        reason_codes.append("COLOR_MATCH")
    elif color1 and color2 and color1 != color2:
        notes.append("Color")
    # One-side color diff: one product has a valid color, the other doesn't
    c1_is_color = is_likely_color(color1)
    c2_is_color = is_likely_color(color2)
    if (c1_is_color and not c2_is_color) or (c2_is_color and not c1_is_color):
        if not color_empty_means_match:
            notes.append("Color")
    # One-side size diff
    s1_is_size = bool(size1) and len(size1) <= 20
    s2_is_size = bool(size2) and len(size2) <= 20
    if (s1_is_size and not s2_is_size) or (s2_is_size and not s1_is_size):
        if "Color" not in notes:
            notes.append("Size")
    # ---- Weight / Ounce comparison ----
    weight1 = clean(attr1.get("weight", ""))
    weight2 = clean(attr2.get("weight", ""))
    # Normalize weight to a comparable form (e.g. "32 oz" → "32oz")
    def _normalize_weight(w: str) -> str:
        if not w:
            return ""
        w = w.strip()
        # Remove commas
        w = w.replace(",", "")
        # Normalize common patterns
        patterns = [
            (re.compile(r"(\d+\.?\d*)\s*(?:oz|ounce)s?\b", re.I), lambda m: f"{float(m.group(1))}oz"),
            (re.compile(r"(\d+\.?\d*)\s*(?:lb|pound|lbs)\b", re.I), lambda m: f"{float(m.group(1))}lb"),
            (re.compile(r"(\d+\.?\d*)\s*(?:kg|kilogram)s?\b", re.I), lambda m: f"{float(m.group(1))}kg"),
            (re.compile(r"(\d+\.?\d*)\s*(?:g|gram|grams)\b", re.I), lambda m: f"{float(m.group(1))}g"),
        ]
        for pat, fmt in patterns:
            m = pat.search(w)
            if m:
                return fmt(m)
        return w
    nw1 = _normalize_weight(weight1)
    nw2 = _normalize_weight(weight2)
    weight_match = bool(nw1 and nw2 and nw1 == nw2)
    if weight_match:
        reasons.append("Weight matched")
        reason_codes.append("WEIGHT_MATCH")
    elif nw1 and nw2 and nw1 != nw2:
        notes.append("Weight")
    # ---- Volume comparison ----
    volume1 = clean(attr1.get("volume", ""))
    volume2 = clean(attr2.get("volume", ""))
    def _normalize_volume(v: str) -> str:
        if not v:
            return ""
        v = v.strip().replace(",", "")
        patterns = [
            (re.compile(r"(\d+\.?\d*)\s*(?:fl\.?\s*oz|fluid\s*ounce)s?\b", re.I), lambda m: f"{float(m.group(1))}floz"),
            (re.compile(r"(\d+\.?\d*)\s*(?:ml|milliliter|millilitre)s?\b", re.I), lambda m: f"{float(m.group(1))}ml"),
            (re.compile(r"(\d+\.?\d*)\s*(?:l|liter|litre)s?\b", re.I), lambda m: f"{float(m.group(1))}L"),
        ]
        for pat, fmt in patterns:
            m = pat.search(v)
            if m:
                return fmt(m)
        return v
    nv1 = _normalize_volume(volume1)
    nv2 = _normalize_volume(volume2)
    volume_match = bool(nv1 and nv2 and nv1 == nv2)
    if volume_match:
        reasons.append("Volume matched")
        reason_codes.append("VOLUME_MATCH")
    elif nv1 and nv2 and nv1 != nv2:
        notes.append("Volume")
    # Determine status
    # Rule 1: UPC match bypass (already handled above)
    # Rule 2: Title-only match (when enabled) — brand match + good title + no critical conflicts
    if enable_title_only_match and brand_match and title_sim >= 0.35:
        status = "EXACT_MATCH"
    # Rule 3: title_sim above threshold + brand_match + no critical conflicts
    elif title_sim >= title_similarity_threshold and brand_match and not any(d in notes for d in ["Size", "Color"]):
        status = "EXACT_MATCH"
    # Rule 4: confidence above threshold
    elif confidence >= exact_match_threshold:
        status = "EXACT_MATCH"
    else:
        status = "NOT_MATCH"
    exact = "Yes" if status == "EXACT_MATCH" else "No"
    reason_str = "; ".join(reasons) if reasons else "No attributes matched"
    return {
        "decision": status,
        "exact_match": exact,
        "reason_code": "_".join(reason_codes) if reason_codes else "NO_MATCH",
        "reason": reason_str,
        "confidence": round(min(confidence, 1.0) * 100, 1),
        "notes": ", ".join(notes) if notes else "",
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
    input_data = load_job_input()
    payload = _resolve_payload(input_data)
    input_path = payload.get("input_path") or payload.get("source_path")
    if not input_path:
        output_result({"success": False, "error": "Missing required payload field: input_path or source_path"})
    url_column1 = payload.get("url_column1", "walmart_url")
    url_column2 = payload.get("url_column2", "comparison_url")
    attributes_column1 = payload.get("attributes_column1", "")
    attributes_column2 = payload.get("attributes_column2", "")
    compare_method = payload.get("compare_method", "similarity")
    output_path = payload.get("output_path", "")
    if not output_path:
        stem = Path(input_path).stem
        output_path = str(Path(input_path).parent / f"{stem}_compared.csv")
    logger.info("Loading %s", input_path)
    df = load_dataframe(input_path)
    logger.info("Loaded %d rows", len(df))
    # Resolve success column names — scraper may output mixed case
    # e.g. "Walmart_Url_success" or "walmart_url_success"
    def _find_col(df_cols, candidates):
        """Find first matching column name from candidates list."""
        lower_map = {c.lower(): c for c in df_cols}
        for c in candidates:
            if c in df_cols:
                return c
            if c.lower() in lower_map:
                return lower_map[c.lower()]
        return None

    results = []
    for _, row in df.iterrows():
        entry = row.to_dict()
        if attributes_column1 and attributes_column2:
            attr1 = parse_attributes(str(row.get(attributes_column1, "{}")))
            attr2 = parse_attributes(str(row.get(attributes_column2, "{}")))
            if attr1 and attr2:
                comparison = compare_attributes(attr1, attr2, payload)

                # Determine scrape success using the _success columns from scraper output
                # (NOT by counting attributes — that caused false404 notes)
                success_col1 = _find_col(df.columns, [
                    f"{attributes_column1.replace('_attributes_json', '')}_success",
                    f"{url_column1}_success",
                    f"{attributes_column1.split('_attributes')[0]}_success",
                ])
                success_col2 = _find_col(df.columns, [
                    f"{attributes_column2.replace('_attributes_json', '')}_success",
                    f"{url_column2}_success",
                    f"{attributes_column2.split('_attributes')[0]}_success",
                ])
                scraped_ok_1 = bool(row.get(success_col1, True)) if success_col1 else True
                scraped_ok_2 = bool(row.get(success_col2, True)) if success_col2 else True

                if not scraped_ok_1 or not scraped_ok_2:
                    # One side failed to scrape →404 note
                    existing = comparison.get("notes", "")
                    note_404 = "Product page not found (404)"
                    comparison["notes"] = f"{existing}; {note_404}" if existing else note_404
                elif scraped_ok_1 and scraped_ok_2:
                    # Both sides scraped OK — check for partial-data exact match
                    filled_1 = sum(1 for v in attr1.values() if v)
                    filled_2 = sum(1 for v in attr2.values() if v)
                    if filled_1 <= 2 or filled_2 <= 2:
                        # One side has minimal data — if titles are similar
                        # and no explicit two-sided conflicts, treat as Exact Match
                        t1 = attr1.get("title", "")
                        t2 = attr2.get("title", "")
                        if t1 and t2:
                            try:
                                t_sim = fuzz.token_sort_ratio(t1, t2) / 100.0
                            except Exception:
                                t_sim = 0.0
                            has_conflict = False
                            c1v = normalize_color_val(attr1.get("color", ""))
                            c2v = normalize_color_val(attr2.get("color", ""))
                            if c1v and c2v and c1v != c2v:
                                has_conflict = True
                            s1v = normalize_size_val(attr1.get("size", ""))
                            s2v = normalize_size_val(attr2.get("size", ""))
                            if s1v and s2v and s1v != s2v:
                                has_conflict = True
                            if t_sim >= 0.5 and not has_conflict:
                                comparison["decision"] = "EXACT_MATCH"
                                comparison["exact_match"] = "Yes"
                                comparison["notes"] = ""
            else:
                comparison = {
                    "decision": "NOT_MATCH",
                    "exact_match": "No",
                    "reason_code": "MISSING_ATTRIBUTES",
                    "reason": "Could not parse attributes from one or both columns",
                    "confidence": 0.0,
                    "notes": "Scrape failed for one or both products",
                }
        else:
            val1 = str(row.get(url_column1, ""))
            val2 = str(row.get(url_column2, ""))
            comparison = compare_strings(val1, val2)
        is_exact = comparison["exact_match"] == "Yes"
        entry["Match_Type"] = "Exact Match" if is_exact else "Incorrect Match"
        entry["Match_Type_Comments"] = "Product and its attributes are matching" if is_exact else "Product attributes are different"
        entry["Notes"] = comparison.get("notes", "")
        results.append(entry)
    result_df = pd.DataFrame(results)
    # Map known column name aliases from scraper output to expected names
    # (scraper may output mixed-case names like "Walmart_Url" vs "walmart_url")
    # Build a lowercase→original mapping for case-insensitive lookup
    lower_col_map = {c.lower(): c for c in result_df.columns}
    col_rename_map = {
        'comp_url': 'Comp_Url',
        'walmart_url': 'Walmart_Url',
        'comparison_url': 'Comp_Url',
    }
    for alias, target in col_rename_map.items():
        orig = lower_col_map.get(alias)
        if orig and orig != target and target not in result_df.columns:
            result_df.rename(columns={orig: target}, inplace=True)
            logger.info("Renamed column '%s' -> '%s'", orig, target)
    # Refresh column map after renames
    lower_col_map = {c.lower(): c for c in result_df.columns}
    # Drop attributes columns (keep only id/url/match columns)
    # Use case-insensitive matching for column names
    def _col(name):
        """Get original-case column name from lowercase hint."""
        return lower_col_map.get(name.lower(), name)
    attributes_col1_orig = lower_col_map.get(attributes_column1.lower()) if attributes_column1 else None
    attributes_col2_orig = lower_col_map.get(attributes_column2.lower()) if attributes_column2 else None
    drop_cols = [c for c in [attributes_col1_orig, attributes_col2_orig] if c and c in result_df.columns]
    if drop_cols:
        result_df.drop(columns=drop_cols, inplace=True)
        logger.info("Dropped intermediate columns: %s", drop_cols)
    # Drop scrape attribute detail columns (keep only id/url/match columns)
    suffix_patterns = ["_success", "_attributes_json", "_title", "_brand", "_upc", "_gtin", "_model", "_size", "_color", "_price", "_pack_count", "_quantity", "_description", "_images_json"]
    scrape_cols = []
    for col in result_df.columns:
        for suffix in suffix_patterns:
            if col.endswith(suffix):
                scrape_cols.append(col)
                break
    if scrape_cols:
        result_df.drop(columns=scrape_cols, inplace=True)
        logger.info("Dropped %d scrape attribute columns", len(scrape_cols))
    # Also drop any raw comparison dict keys that leaked through
    raw_comp_cols = [c for c in result_df.columns if c.lower() in ("exact_match", "reason_code", "reason", "confidence", "decision")]
    if raw_comp_cols:
        result_df.drop(columns=raw_comp_cols, inplace=True)
        logger.info("Dropped raw comparison columns: %s", raw_comp_cols)
    # Drop any leftover lowercase match_type / notes columns that came from entry dict
    # (the entry dict uses Match_Type / Notes with PascalCase — those are the ones we want)

    # Transform to client output format (Validation.xlsx style)
    # Get the Walmart and Comp URL columns from the dataframe
    walmart_col = lower_col_map.get("walmart_url", "Walmart_Url")
    comp_col = lower_col_map.get("comp_url", lower_col_map.get("comparison_url", "Comp_Url"))
    result_df["prmry_sku_id"] = result_df[walmart_col].apply(
        lambda u: extract_walmart_id(str(u)))
    result_df["Walmart_Url"] = result_df[walmart_col].apply(
        lambda u: format_walmart_url(str(u)))
    result_df["comp_item_id__1"] = result_df[comp_col].apply(
        lambda u: extract_amazon_asin(str(u)))
    result_df["Comp_Url"] = result_df[comp_col].apply(
        lambda u: format_comp_url(str(u)))
    # Drop old URL columns
    for col in [walmart_col, comp_col, "product_id"]:
        if col in result_df.columns and col not in ("Walmart_Url", "Comp_Url"):
            result_df.drop(columns=[col], inplace=True)
    # Ensure client-exact column order and names (PascalCase as in Validation.xlsx)
    client_cols = ["prmry_sku_id", "Walmart_Url", "comp_item_id__1", "Comp_Url",
                   "Match_Type", "Match_Type_Comments", "Notes"]
    # The entry dict writes Match_Type / Match_Type_Comments / Notes with PascalCase
    # but pd.DataFrame may have normalized them. Re-apply from results list.
    for i, r in enumerate(results):
        if "Match_Type" in result_df.columns:
            result_df.loc[result_df.index[i], "Match_Type"] = r.get("Match_Type", "")
            result_df.loc[result_df.index[i], "Match_Type_Comments"] = r.get("Match_Type_Comments", "")
            result_df.loc[result_df.index[i], "Notes"] = r.get("Notes", "")
    # Drop any lowercase variants that leaked from the entry dict
    for lc in ["match_type", "match_type_comments", "notes"]:
        if lc in result_df.columns and lc not in client_cols:
            result_df.drop(columns=[lc], inplace=True)
    # Drop extra columns not in client format
    extra = [c for c in result_df.columns if c not in client_cols]
    if extra:
        result_df.drop(columns=extra, inplace=True)
        logger.info("Dropped %d extra columns: %s", len(extra), extra)
    # Ensure exact column order
    result_df = result_df[[c for c in client_cols if c in result_df.columns]]
    result_df.to_csv(output_path, index=False)
    logger.info("Wrote %d rows to %s", len(results), output_path)
    output_result({
        "success": True,
        "data": {
            "processed_rows": len(results),
            "output_path": output_path,
            "metadata": {
                "method": compare_method,
                "attribute_mode": bool(attributes_column1 and attributes_column2),
                "exact_matches": sum(1 for r in results if r.get("Match_Type") == "Exact Match"),
                "not_matches": sum(1 for r in results if r.get("Match_Type") == "Incorrect Match"),
            },
        },
        "error": None,
    })

if __name__ == "__main__":
    main()
