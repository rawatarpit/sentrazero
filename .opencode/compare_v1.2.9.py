#!/usr/bin/env python3
import sys
import json
import logging
import re
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

EXACT_MATCH_THRESHOLD = 0.60
MATCH_THRESHOLD = 0.40

# Known color names (used to validate whether a "color" field is a real color)
KNOWN_COLORS = {
    "black", "white", "red", "blue", "green", "yellow", "orange", "purple",
    "pink", "brown", "gray", "grey", "navy", "teal", "beige", "tan",
    "gold", "silver", "bronze", "copper", "ivory", "cream", "charcoal",
    "chocolate", "rose", "lime", "turquoise", "magenta", "violet",
    "indigo", "coral", "khaki", "maroon", "burgundy", "mint", "peach",
    "lavender", "lilac", "crimson", "ruby", "sapphire", "emerald",
    "plum", "olive", "aqua", "cyan", "fuchsia", "blush", "mocha",
}


def is_likely_color(text: str) -> bool:
    """Check if a string looks like a real color value (not a product description)."""
    if not text or len(text) > 60:
        return False
    lower = text.lower().strip()
    # If it matches exactly a known color name
    if lower in KNOWN_COLORS:
        return True
    # If it contains a known color word and is short
    words = lower.split()
    if len(words) <= 4:
        for word in words:
            word = word.strip(" ,.()[]-")
            if word in KNOWN_COLORS:
                return True
    # If the string is very short, treat as potential color
    if len(lower) <= 15:
        return True
    return False


def output_csv_row(data: Dict[str, Any], output_file: str) -> None:
    """Write a single CSV row to output file."""
    pass  # handled by pandas


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
    try:
        return pd.read_csv(path) if p.suffix == ".csv" else pd.read_excel(path)
    except Exception as e:
        print(json.dumps({"success": False, "error": f"Failed to read input: {e}"}))
        sys.exit(1)


def output_result(result: Dict[str, Any]) -> None:
    print(json.dumps(result))
    sys.exit(0)


def clean(text: Any) -> str:
    return " ".join(str(text).lower().strip().split())


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
        (re.compile(r"(\d+)\s*(count|ct|pack)", re.I),
         lambda m: f"{m.group(1)}ct"),
    ]
    for pat, fmt in patterns:
        m = pat.search(text)
        if m:
            return fmt(m)
    return text


def parse_attributes(attrs_json: str) -> Dict[str, Any]:
    try:
        attrs = json.loads(attrs_json) if attrs_json and attrs_json != "{}" else {}
        if isinstance(attrs, dict):
            result = {
                "title": str(attrs.get("title", "")),
                "brand": str(attrs.get("brand", "")),
                "upc": str(attrs.get("upc", "")),
                "gtin": str(attrs.get("gtin", "")),
                "size": str(attrs.get("size", "")),
                "color": str(attrs.get("color", "")),
                "model": str(attrs.get("model", "")),
                "price": float(attrs.get("price", 0.0)),
            }
            # Preserve scrape error info if present
            if "_scrape_error" in attrs:
                result["_scrape_error"] = str(attrs["_scrape_error"])
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    return {}


def normalize_brand(text: str) -> str:
    """Strip 'Visit the ... Store' wrappers from brand names."""
    t = clean(text)
    t = re.sub(r'^visit the\s+', '', t)
    t = re.sub(r'\s+store$', '', t)
    t = re.sub(r'\s+store\s+store$', ' store', t)
    return t.strip()


def compare_attributes(attr1: Dict[str, Any], attr2: Dict[str, Any]) -> Dict[str, Any]:
    upc1 = clean(attr1.get("upc", ""))
    upc2 = clean(attr2.get("upc", ""))
    brand1_raw = clean(attr1.get("brand", ""))
    brand2_raw = clean(attr2.get("brand", ""))
    brand1 = normalize_brand(attr1.get("brand", ""))
    brand2 = normalize_brand(attr2.get("brand", ""))
    title1 = attr1.get("title", "")
    title2 = attr2.get("title", "")
    size1 = normalize_size_val(attr1.get("size", ""))
    size2 = normalize_size_val(attr2.get("size", ""))
    color1 = normalize_color_val(attr1.get("color", ""))
    color2 = normalize_color_val(attr2.get("color", ""))

    # Track which attributes are meaningfully present on each side
    has_any_attr1 = bool(title1 or brand1 or upc1 or size1 or color1)
    has_any_attr2 = bool(title2 or brand2 or upc2 or size2 or color2)

    # --- compute confidence across ALL fields ---
    confidence = 0.0

    # UPC (exact)
    upc_match = bool(upc1 and upc2 and upc1 == upc2)
    upc_on_one_side = (bool(upc1) and not bool(upc2)) or (bool(upc2) and not bool(upc1))
    if upc_match:
        confidence += WEIGHTS["upc"]

    # Brand (fuzzy match after normalization)
    brand_match = False
    if brand1 and brand2:
        try:
            brand_sim = fuzz.token_sort_ratio(brand1, brand2) / 100.0
        except Exception:
            brand_sim = fuzz.ratio(brand1, brand2) / 100.0
        if brand_sim >= 0.80:
            brand_match = True
            confidence += WEIGHTS["brand"]

    # Title (fuzzy)
    title_sim = 0.0
    if title1 and title2:
        try:
            title_sim = fuzz.token_sort_ratio(title1, title2) / 100.0
        except Exception:
            title_sim = fuzz.ratio(title1, title2) / 100.0
        confidence += WEIGHTS["title"] * title_sim

    # Size (exact after normalization)
    size_match = bool(size1 and size2 and size1 == size2)
    if size_match:
        confidence += WEIGHTS["size"]

    # Color (exact after normalization)
    color_match = bool(color1 and color2 and color1 == color2)
    if color_match:
        confidence += WEIGHTS["color"]

    # --- SMART DIFF DETECTION ---
    # Only flag color/size diffs when the data looks meaningful
    meaningful_diffs = []
    secondary_diffs = []

    # Color: only flag when both sides have likely-real color values
    c1_is_color = is_likely_color(color1) if color1 else False
    c2_is_color = is_likely_color(color2) if color2 else False
    if c1_is_color and c2_is_color and color1 != color2:
        meaningful_diffs.append("Color")

    # Size: flag when both sides have size and differ
    if size1 and size2 and size1 != size2:
        meaningful_diffs.append("Size")
    # One-side color: flag when one side has a real color and the other doesn't
    # (e.g. Walmart specs have "Pink" but Amazon color field has a product description)
    if (c1_is_color and not c2_is_color) or (c2_is_color and not c1_is_color):
        secondary_diffs.append("Color")
    # One-side size: no strong conclusion, but surfaces the diff when
    # no other meaningful diff exists (e.g., curtain sizes on one side only)
    if (size1 and not size2) or (size2 and not size1):
        secondary_diffs.append("Size")

    has_diffs = bool(meaningful_diffs or secondary_diffs)
    has_real_diffs = bool(meaningful_diffs)

    # Compute notes priority: meaningful > secondary > generic
    notes_attr = meaningful_diffs[0] if meaningful_diffs else (
                secondary_diffs[0] if secondary_diffs else "Attributes")

    # --- DECISION LOGIC ---
    if upc_match:
        status = "EXACT_MATCH"
        exact = "Yes"
        reason = "Product and its attributes are matching"
        notes = ""
    elif confidence >= EXACT_MATCH_THRESHOLD and not has_real_diffs and not secondary_diffs:
        # Overall confidence high and no meaningful attribute conflicts
        status = "EXACT_MATCH"
        exact = "Yes"
        reason = "Product and its attributes are matching"
        notes = ""
    elif title_sim >= 0.80 and not has_real_diffs and not secondary_diffs:
        status = "EXACT_MATCH"
        exact = "Yes"
        reason = "Product and its attributes are matching"
        notes = ""
    elif title_sim >= 0.65 and brand_match and not has_real_diffs and not secondary_diffs:
        status = "EXACT_MATCH"
        exact = "Yes"
        reason = "Product and its attributes are matching"
        notes = ""
    elif title_sim >= 0.50 and brand_match and size_match and color_match:
        # All key attributes match (brand, size, color) with decent title
        status = "EXACT_MATCH"
        exact = "Yes"
        reason = "Product and its attributes are matching"
        notes = ""
    elif upc_on_one_side and title_sim >= 0.50 and not secondary_diffs:
        # UPC on one side validates product identity even when brand differs
        status = "EXACT_MATCH"
        exact = "Yes"
        reason = "Product and its attributes are matching"
        notes = ""
    else:
        status = "NOT_MATCH"
        exact = "No"
        reason = "Product attributes are different"
        notes = notes_attr

    return {
        "decision": status,
        "exact_match": exact,
        "reason_code": "MATCH" if exact == "Yes" else "MISMATCH",
        "reason": reason,
        "notes": notes,
        "confidence": round(min(confidence, 1.0) * 100, 1),
        "title_sim": round(title_sim, 3),
        "brand_match": brand_match,
        "upc_match": upc_match,
        "has_diffs": has_diffs,
        "_raw_attr1": attr1,
        "_raw_attr2": attr2,
    }


def compare_strings(val1: str, val2: str) -> Dict[str, Any]:
    s1, s2 = clean(val1), clean(val2)
    if not s1 or not s2:
        return {
            "decision": "NOT_MATCH",
            "exact_match": "No",
            "reason_code": "NO_MATCH",
            "reason": "One or both values empty",
            "confidence": 0.0,
        }
    if s1 == s2:
        return {
            "decision": "EXACT_MATCH",
            "exact_match": "Yes",
            "reason_code": "EXACT_STRING",
            "reason": "Exact string match",
            "confidence": 100.0,
        }
    try:
        sim = fuzz.token_sort_ratio(s1, s2) / 100.0
    except Exception:
        sim = fuzz.ratio(s1, s2) / 100.0
    if sim >= 0.9:
        return {
            "decision": "EXACT_MATCH",
            "exact_match": "Yes",
            "reason_code": "HIGH_SIMILARITY",
            "reason": f"String similarity {sim:.0%}",
            "confidence": round(sim * 100, 1),
        }
    return {
        "decision": "NOT_MATCH",
        "exact_match": "No",
        "reason_code": "LOW_SIMILARITY",
        "reason": f"String similarity {sim:.0%}",
        "confidence": round(sim * 100, 1),
    }


def main() -> None:
    input_data = load_job_input()
    payload = input_data.get("payload", {})
    input_path = payload.get("input_path")
    if not input_path:
        output_result({"success": False, "error": "Missing required payload field: input_path"})
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
    results = []
    for _, row in df.iterrows():
        entry = row.to_dict()
        if attributes_column1 and attributes_column2:
            attr1 = parse_attributes(str(row.get(attributes_column1, "{}")))
            attr2 = parse_attributes(str(row.get(attributes_column2, "{}")))
            if attr1 and attr2:
                # Check how much attribute data each side has
                has_any1 = any(attr1.get(k) for k in ["title", "brand", "upc", "size", "color"])
                has_any2 = any(attr2.get(k) for k in ["title", "brand", "upc", "size", "color"])
                if has_any1 and has_any2:
                    # Both sides have data → full attribute comparison
                    comparison = compare_attributes(attr1, attr2)
                elif has_any1 or has_any2:
                    # One side has NO scraped data (scrape failure)
                    # Check if the failure is due to a known scrape error
                    scrape_error1 = attr1.get("_scrape_error", "")
                    scrape_error2 = attr2.get("_scrape_error", "")
                    scrape_error = scrape_error1 or scrape_error2
                    scrape_note = ""
                    if scrape_error1:
                        scrape_note = f"Walmart scrape: {scrape_error1}"
                    elif scrape_error2:
                        scrape_note = f"Amazon scrape: {scrape_error2}"
                    # Try to use available title data for comparison
                    val1 = str(row.get(url_column1 + "_title", ""))
                    val2 = str(row.get(url_column2 + "_title", ""))
                    if val1 and val2:
                        comp = compare_strings(val1, val2)
                        # Also do attribute comparison with what we have to get debug info
                        attr_comp = compare_attributes(attr1, attr2)
                        comparison = comp
                        comparison["notes"] = comp.get("notes", "") + (f" ({scrape_note})" if scrape_note else " (limited scrape data)")
                        comparison["scrape_error"] = scrape_error
                        # Propagate debug info
                        comparison["title_sim"] = comp.get("title_sim", attr_comp.get("title_sim", 0))
                        comparison["brand_match"] = attr_comp.get("brand_match", False)
                        comparison["upc_match"] = attr_comp.get("upc_match", False)
                        comparison["has_diffs"] = attr_comp.get("has_diffs", False)
                        comparison["confidence"] = comp.get("confidence", attr_comp.get("confidence", 0))
                    else:
                        # No title data either → fall back to URL comparison
                        val1 = str(row.get(url_column1, ""))
                        val2 = str(row.get(url_column2, ""))
                        comparison = compare_strings(val1, val2)
                        note = f"no attribute data"
                        if scrape_note:
                            note += f" - {scrape_note}"
                        comparison["notes"] = comparison.get("notes", "") + f" ({note})"
                        comparison["scrape_error"] = scrape_error
                else:
                    # Both sides have empty attributes → fall back to URL string comparison
                    scrape_error1 = attr1.get("_scrape_error", "")
                    scrape_error2 = attr2.get("_scrape_error", "")
                    scrape_error = scrape_error1 or scrape_error2
                    scrape_parts = []
                    if scrape_error1:
                        scrape_parts.append(f"Walmart: {scrape_error1}")
                    if scrape_error2:
                        scrape_parts.append(f"Amazon: {scrape_error2}")
                    note = "no scraped attributes"
                    if scrape_parts:
                        note += " - " + "; ".join(scrape_parts)
                    val1 = str(row.get(url_column1, ""))
                    val2 = str(row.get(url_column2, ""))
                    comparison = compare_strings(val1, val2)
                    comparison["notes"] = comparison.get("notes", "") + f" ({note})"
                    comparison["scrape_error"] = scrape_error
            else:
                comparison = {
                    "decision": "NOT_MATCH",
                    "exact_match": "No",
                    "reason_code": "MISSING_ATTRIBUTES",
                    "reason": "Could not parse attributes from one or both columns",
                    "confidence": 0.0,
                    "notes": "Scrape failed for one or both products",
                    "has_diffs": False,
                    "title_sim": 0,
                    "brand_match": False,
                    "upc_match": False,
                    "_raw_attr1": attr1,
                    "_raw_attr2": attr2,
                }
        else:
            val1 = str(row.get(url_column1, ""))
            val2 = str(row.get(url_column2, ""))
            comparison = compare_strings(val1, val2)
        entry["Match_Type"] = "Exact Match" if comparison["exact_match"] == "Yes" else "Incorrect Match"
        entry["Match_Type_Comments"] = comparison["reason"]
        entry["Notes"] = comparison.get("notes", "")
        # Debug columns
        entry["_debug_title_sim"] = comparison.get("title_sim", 0)
        entry["_debug_brand_match"] = comparison.get("brand_match", False)
        entry["_debug_upc_match"] = comparison.get("upc_match", False)
        entry["_debug_has_diffs"] = comparison.get("has_diffs", False)
        entry["_debug_confidence"] = comparison.get("confidence", 0)
        # Scrape error info (available when one side failed to scrape)
        entry["_debug_scrape_error"] = comparison.get("scrape_error", "")
        # Raw scraped attribute values
        if "_raw_attr1" in comparison:
            ra1 = comparison["_raw_attr1"]
            ra2 = comparison["_raw_attr2"]
            entry["_attr1_title"] = ra1.get("title", "")
            entry["_attr1_brand"] = ra1.get("brand", "")
            entry["_attr1_upc"] = ra1.get("upc", "")
            entry["_attr1_size"] = ra1.get("size", "")
            entry["_attr1_color"] = ra1.get("color", "")
            entry["_attr1_price"] = ra1.get("price", 0.0)
            entry["_attr2_title"] = ra2.get("title", "")
            entry["_attr2_brand"] = ra2.get("brand", "")
            entry["_attr2_upc"] = ra2.get("upc", "")
            entry["_attr2_size"] = ra2.get("size", "")
            entry["_attr2_color"] = ra2.get("color", "")
            entry["_attr2_price"] = ra2.get("price", 0.0)
        results.append(entry)
    result_df = pd.DataFrame(results)
    # Only drop the raw JSON attribute blobs (bulky) and internal comparison fields
    drop_cols = [col for col in [attributes_column1, attributes_column2] if col in result_df.columns]
    if drop_cols:
        result_df.drop(columns=drop_cols, inplace=True)
        logger.info("Dropped intermediate columns: %s", drop_cols)
    raw_comp_cols = [c for c in ["exact_match", "reason_code", "reason", "confidence", "decision", "notes",
                                   "_raw_attr1", "_raw_attr2"] if c in result_df.columns]
    if raw_comp_cols:
        result_df.drop(columns=raw_comp_cols, inplace=True)
        logger.info("Dropped raw comparison columns: %s", raw_comp_cols)
    # Transform to client output format
    result_df["prmry_sku_id"] = result_df.get("walmart_url", result_df.get("Walmart_Url", pd.Series(""))).apply(
        lambda u: extract_walmart_id(str(u)))
    result_df["Walmart_Url"] = result_df.get("walmart_url", result_df.get("Walmart_Url", pd.Series(""))).apply(
        lambda u: format_walmart_url(str(u)))
    result_df["comp_item_id__1"] = result_df.get("comparison_url", result_df.get("Comp_Url", pd.Series(""))).apply(
        lambda u: extract_amazon_asin(str(u)))
    result_df["Comp_Url"] = result_df.get("comparison_url", result_df.get("Comp_Url", pd.Series(""))).apply(
        lambda u: format_comp_url(str(u)))
    # Drop old raw url/id columns
    for col in ["product_id", "walmart_url", "comparison_url"]:
        if col in result_df.columns:
            result_df.drop(columns=[col], inplace=True)
    # Write all remaining columns
    result_df.to_csv(output_path, index=False)
    logger.info("Wrote %d rows with %d columns to %s", len(results), len(result_df.columns), output_path)
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
