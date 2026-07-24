#!/usr/bin/env python3
"""
dup_classify.py — Step 2 of Walmart Duplicate Detection Pipeline

For each product in the search results CSV:
  1. Reads candidate products found by walmart_search
  2. Compares original product attributes with candidate attributes
  3. Classifies:
     - Duplicate (has_dups=Yes) — if candidate is the same product 
     - Not duplicate (has_dups=No) — with reason: Different design/color/size/material/Isbn

Output matches the Baselining Output format:
  product_id, item_id, pt, brand, product_name, product_lifecycle_status,
  product_class_type, product_publish_status, primary_image_url,
  Item has published dups ?, count of published dups,
  published_dup_1, published_dup_2, published_dup_3, Comments, Remarks
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("dup_classify")

try:
    import pandas as pd
except ImportError as e:
    print(json.dumps({"success": False, "error": f"Missing dependency: {e}"}))
    sys.exit(1)

# ── Color / size / material / design dictionaries ─────────────────────

COLOR_WORDS = {
    "alabaster","almond","apricot","aqua","aquamarine","ash","azure","beige",
    "black","blue","brown","burgundy","camel","cappuccino","caramel","cayenne",
    "champagne","charcoal","cherry","chestnut","chili","chocolate","cinnamon",
    "cobalt","coffee","coral","cranberry","cream","crimson","cyan","denim",
    "ebony","ecru","eggplant","emerald","espresso","fuchsia","ginger","gold",
    "graphite","gray","green","grey","gunmetal","indigo","ivory","jade","khaki",
    "lavender","lemon","lilac","lime","magenta","mahogany","maroon","mauve",
    "midnight","mint","mocha","mustard","navy","oak","olive","onyx","orange",
    "orchid","peach","pearl","periwinkle","pink","platinum","plum","purple",
    "raspberry","red","rose","ruby","sage","salmon","sapphire","scarlet",
    "silver","skyblue","slate","smoke","snow","steel","stone","tan","taupe",
    "teal","terracotta","titanium","turquoise","violet","walnut","wheat",
    "white","wine","yellow",
}

SIZE_WORDS = {
    "xs","sm","md","lg","xl","xxl","3xl","4xl","5xl","6xl",
    "small","medium","large","x-large","xx-large","xxx-large",
    "28","29","30","31","32","33","34","35","36","37","38","39","40",
    "41","42","43","44","46","48","50","52","54","56","58","60",
    "5","5.5","6","6.5","7","7.5","8","8.5","9","9.5","10","10.5",
    "11","11.5","12","13","14","15","16",
    "king","queen","twin","full","california king",
    "oz","lb","gallon","quart","pint","liter","ml",
}

MATERIAL_WORDS = {
    "cotton","polyester","leather","denim","wool","silk","linen","nylon",
    "spandex","elastane","rayon","viscose","acrylic","fleece","velvet",
    "suede","canvas","mesh","lace","satin","lace","cashmere","chiffon",
    "corduroy","flannel","jersey","knit","tweed","twill","polyurethane",
    "rubber","silicone","ceramic","porcelain","glass","crystal","plastic",
    "metal","wood","bamboo","marble","granite","stone","paper","cardboard",
    "stainless steel","aluminum","copper","brass","iron","steel","gold",
    "silver","leather","faux leather","vegan leather",
}

DESIGN_WORDS = {
    "floral","striped","plaid","checkered","polka dot","polka","geometric",
    "abstract","graphic","solid","printed","pattern","camouflage","camo",
    "tie-dye","ombre","gradient","animal print","leopard","zebra","tiger",
    "snake","paisley","herringbone","houndstooth","argyle","chevron",
    "diamond","dot","stripe","check","plaid","tartan","gingham","batik",
    "ikat","tropical","nautical","vintage","retro","modern","classic",
    "sleeveless","hooded","zip-up","pullover","button-down","crew neck",
    "v-neck","turtleneck","mock neck","collar","cuff","pocket","zipper",
    "lace-up","slip-on","velcro","buckle","drawstring","elastic",
}

ISBN_PATTERN = re.compile(r'\b(?:isbn|isbn-10|isbn-13)[:\s]*[\d-]{9,17}', re.IGNORECASE)
ISBN_BARE_PATTERN = re.compile(r'\b(?:97[89][\d]{10}|[\d]{10})\b')  # ISBN-13 (978/979 + 10 digits) or ISBN-10 (10 digits)


# ── Helpers ────────────────────────────────────────────────────────────

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
    # Handle directory input: scan for CSV/XLSX files
    if p.is_dir():
        logger.info("Input is a directory, scanning for data files in %s", path)
        csv_files = list(p.glob("*.csv")) + list(p.glob("*.CSV")) + list(p.glob("*.xlsx")) + list(p.glob("*.XLSX"))
        if not csv_files:
            # Try subdirectories (compound step outputs)
            for sub in p.iterdir():
                if sub.is_dir():
                    csv_files.extend(sub.glob("*.csv"))
                    csv_files.extend(sub.glob("*.CSV"))
                    csv_files.extend(sub.glob("*.xlsx"))
                    csv_files.extend(sub.glob("*.XLSX"))
        if not csv_files:
            print(json.dumps({"success": False, "error": f"No CSV/XLSX files found in directory: {path}"}))
            sys.exit(1)
        # Use the first CSV found (sorted for determinism)
        csv_files.sort(key=lambda f: str(f))
        p = csv_files[0]
        logger.info("Directory input resolved to file: %s", p)
    try:
        return pd.read_csv(p)
    except Exception:
        pass
    try:
        return pd.read_excel(p)
    except Exception as e:
        print(json.dumps({"success": False, "error": f"Failed to read input: {e}"}))
        sys.exit(1)


def output_result(result: Dict[str, Any]) -> None:
    print(json.dumps(result))
    sys.exit(0)


def clean(text: Any) -> str:
    return " ".join(str(text).lower().strip().split())


def tokenize(text: str) -> set:
    return set(clean(text).split())


def extract_color_words(text: str) -> set:
    """Return set of color words found in text."""
    words = tokenize(text)
    return {w for w in words if w in COLOR_WORDS}


def extract_size_words(text: str) -> set:
    """Return set of size-related words found in text."""
    words = tokenize(text)
    return {w for w in words if w in SIZE_WORDS}


def extract_material_words(text: str) -> set:
    """Return set of material words found in text."""
    words = tokenize(text)
    return {w for w in words if w in MATERIAL_WORDS}


def extract_design_words(text: str) -> set:
    """Return set of design/style words found in text."""
    words = tokenize(text)
    return {w for w in words if w in DESIGN_WORDS}


def has_isbn(text: str) -> bool:
    """Check if text contains ISBN reference."""
    if ISBN_PATTERN.search(text):
        return True
    # Also check for bare ISBN-13 numbers (978/979 prefix)
    if ISBN_BARE_PATTERN.search(text):
        return True
    return False


def jaccard_similarity(t1: str, t2: str) -> float:
    """Jaccard similarity of two title strings."""
    s1 = tokenize(t1)
    s2 = tokenize(t2)
    if not s1 or not s2:
        return 0.0
    return len(s1 & s2) / len(s1 | s2)


def classify_pair(product_row: Dict[str, Any], candidate_row: Optional[Dict[str, Any]]) -> Tuple[str, str, str, str]:
    """
    Classify a product-candidate pair.
    
    Returns: (has_dups, comments, dup_id, remarks)
      has_dups: "Yes" | "No"
      comments: reason string
      dup_id: candidate product_id if duplicate, else ""
      remarks: additional notes
    """
    product_name = product_row.get("product_name", "")
    product_brand = product_row.get("brand", "")
    pt = product_row.get("pt", "")

    if not candidate_row:
        # No candidate was found at all
        remarks = "Not active on walmart page" if "not active" in str(product_row.get("Remarks", "")).lower() else ""
        return "No", "No duplicate found", "", remarks

    candidate_title = candidate_row.get("top_candidate_title", "")
    candidate_brand = candidate_row.get("top_candidate_brand", "")
    candidate_url = candidate_row.get("top_candidate_url", "")
    candidate_id = candidate_row.get("top_candidate_id", "")
    similarity = float(candidate_row.get("max_similarity", 0))

    # If no candidate title scraped, can't classify
    if not candidate_title:
        return "No", "No duplicate found", "", ""

    # Brand match check — fuzzy matching for partial/substring matches
    brand_match = False
    if product_brand and candidate_brand:
        pb = clean(product_brand)
        cb = clean(candidate_brand)
        brand_match = (pb == cb) or (pb in cb) or (cb in pb) or (pb.split()[0] == cb.split()[0] if pb.split() and cb.split() else False)

    logger.info(
        "  Classifying: brand_match=%s, similarity=%.3f, title1=%s, title2=%s",
        brand_match, similarity,
        product_name[:50], candidate_title[:50],
    )

    # Extract features from both titles
    prod_colors = extract_color_words(product_name)
    prod_sizes = extract_size_words(product_name)
    prod_materials = extract_material_words(product_name)
    prod_designs = extract_design_words(product_name)

    cand_colors = extract_color_words(candidate_title)
    cand_sizes = extract_size_words(candidate_title)
    cand_materials = extract_material_words(candidate_title)
    cand_designs = extract_design_words(candidate_title)

    # ── ISBN check (highest priority: if both are books with different ISBNs, not a dup)
    if has_isbn(product_name) and has_isbn(candidate_title):
        prod_isbns = set(re.findall(r'(\d{10,13})', product_name))
        cand_isbns = set(re.findall(r'(\d{10,13})', candidate_title))
        if prod_isbns and cand_isbns and prod_isbns != cand_isbns:
            return "No", "Different Isbn", "", ""

    # ── Attribute difference detection — works at ANY similarity level if brand matches
    # These detect cases where the SAME product family differs in one attribute
    if brand_match:
        # Different size
        if prod_sizes and cand_sizes and prod_sizes != cand_sizes:
            return "No", "Different size", "", ""
        # Different material
        if prod_materials and cand_materials and prod_materials != cand_materials:
            return "No", "Different material", "", ""
        # Different color (check even if one side is missing color — implied difference)
        if prod_colors and cand_colors and prod_colors != cand_colors:
            return "No", "Different color", "", ""
        # Different design
        if prod_designs and cand_designs and prod_designs != cand_designs:
            return "No", "Different design", "", ""

    # ── Now decide based on similarity level
    # Very high similarity + brand match → duplicate (titles are essentially identical)
    if similarity >= 0.70 and brand_match:
        return "Yes", "", candidate_id, ""

    # High similarity (0.5-0.70) with brand match
    if similarity >= 0.5 and brand_match:
        # Check if one title contains the other (indicates same product)
        cn = clean(product_name)
        cc = clean(candidate_title)
        cn_in_cc = cn in cc
        cc_in_cn = cc in cn
        if cn_in_cc or cc_in_cn:
            # Find what differs between the two strings
            longer = cc if cn_in_cc else cn
            shorter = cn if cn_in_cc else cc
            diff_tokens = set(longer.split()) - set(shorter.split())
            diff_colors = extract_color_words(" ".join(diff_tokens))
            diff_sizes = extract_size_words(" ".join(diff_tokens))
            diff_materials = extract_material_words(" ".join(diff_tokens))
            diff_designs = extract_design_words(" ".join(diff_tokens))
            # If the only difference is a color/size/material → not a duplicate
            if diff_colors and not diff_sizes and not diff_materials:
                return "No", "Different color", "", ""
            if diff_sizes and not diff_colors and not diff_materials:
                return "No", "Different size", "", ""
            if diff_materials and not diff_colors and not diff_sizes:
                return "No", "Different material", "", ""
            if diff_designs and not diff_colors and not diff_sizes and not diff_materials:
                return "No", "Different design", "", ""
            # Otherwise, the extra words are meaningful → duplicate
            return "Yes", "", candidate_id, ""
        # Check if similarity is very high (mostly stop words differ)
        if similarity >= 0.65:
            return "Yes", "", candidate_id, ""
        # Otherwise there's a meaningful difference
        if prod_designs or cand_designs:
            return "No", "Different design", "", ""
        # Check for other attribute differences even without explicit brand match
        if prod_sizes != cand_sizes and prod_sizes and cand_sizes:
            return "No", "Different size", "", ""
        if prod_colors != cand_colors and prod_colors and cand_colors:
            return "No", "Different color", "", ""
        if prod_materials != cand_materials and prod_materials and cand_materials:
            return "No", "Different material", "", ""
        return "No", "Different design", "", ""

    # Moderate similarity (0.3-0.5) with brand match — attribute difference case
    if similarity >= 0.3 and brand_match:
        # Already checked colors/sizes/materials/designs above
        # If none of those differed, it's still a different product
        return "No", "Different design", "", ""

    # Low similarity or different brand → no match
    return "No", "No duplicate found", "", ""


def build_output_row(product_row: Dict[str, Any], classification: Dict[str, str]) -> Dict[str, Any]:
    """Build an output row matching the Baselining Output format."""
    has_dups = classification["has_dups"]
    comments = classification["comments"]
    dup_id = classification["dup_id"]
    remarks = classification["remarks"]

    # "count of published dups" is "Yes" only when we found a duplicate
    count = "1" if has_dups == "Yes" else ""

    # If has_dups=Yes, dup_id goes in published_dup_1
    dup_1 = dup_id if has_dups == "Yes" else ""
    dup_2 = ""
    dup_3 = ""

    # If no duplicate and no specific comment, default to "No duplicate found"
    if has_dups == "No" and not comments:
        comments = "No duplicate found"

    # Map original columns — preserve everything from input
    row = dict(product_row)

    # Ensure standard output columns
    output = {
        "product_id": row.get("product_id", ""),
        "item_id": row.get("item_id", ""),
        "pt": row.get("pt", row.get("product_class_type", "")),
        "brand": row.get("brand", ""),
        "product_name": row.get("product_name", ""),
        "product_lifecycle_status": row.get("product_lifecycle_status", row.get("lifecycle", "")),
        "product_class_type": row.get("product_class_type", row.get("pt", row.get("class_type", ""))),
        "product_publish_status": row.get("product_publish_status", row.get("publish_status", "")),
        "primary_image_url": row.get("primary_image_url", ""),
        "Item has published dups ?": has_dups,
        "count of published dups": count,
        "published_dup_1": dup_1,
        "published_dup_2": dup_2,
        "published_dup_3": dup_3,
        "Comments": comments,
        "Remarks": remarks,
    }
    return output


# ── Main ───────────────────────────────────────────────────────────────

def main() -> None:
    input_data = load_job_input()
    payload = input_data.get("payload", input_data)
    config = payload.get("config", {})

    input_path = payload.get("input_path") or payload.get("source_path", "")
    output_path = payload.get("output_path", "")
    if not input_path:
        output_result({"success": False, "error": "Missing input_path"})
    if not output_path:
        stem = Path(input_path).stem
        output_path = str(Path(input_path).parent / f"{stem}_classified.csv")

    candidate_threshold = float(config.get("candidate_threshold", 0.3))

    logger.info("Loading input: %s", input_path)
    df = load_dataframe(input_path)
    logger.info("Loaded %d products", len(df))

    output_rows = []
    for idx, row in df.iterrows():
        product_row = dict(row)

        # Determine if candidate exists
        candidate_count = int(product_row.get("candidate_count", 0))
        candidate_id = str(product_row.get("top_candidate_id", ""))
        candidate_row = None

        if candidate_count > 0 and candidate_id:
            candidate_row = product_row  # candidate info is in the same row

        logger.info("[%d/%d] Classifying product %s...", idx + 1, len(df), product_row.get("product_id", ""))

        has_dups, comments, dup_id, remarks = classify_pair(product_row, candidate_row)

        classification = {
            "has_dups": has_dups,
            "comments": comments,
            "dup_id": dup_id,
            "remarks": remarks,
        }

        output_row = build_output_row(product_row, classification)
        output_rows.append(output_row)

    result_df = pd.DataFrame(output_rows)
    result_df.to_csv(output_path, index=False)
    logger.info("Wrote %d rows to %s", len(result_df), output_path)

    # Summary
    dups_count = sum(1 for r in output_rows if r["Item has published dups ?"] == "Yes")
    no_dups_count = sum(1 for r in output_rows if r["Item has published dups ?"] == "No")

    output_result({
        "success": True,
        "data": {
            "total_products": len(df),
            "duplicates_found": dups_count,
            "non_duplicates": no_dups_count,
            "output_path": output_path,
            "metadata": {
                "candidate_threshold": candidate_threshold,
            },
        },
        "error": None,
    })


if __name__ == "__main__":
    main()
