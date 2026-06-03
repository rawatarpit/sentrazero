#!/usr/bin/env python3
import sys
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("compare")


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
        logger.error("Input file not found: %s", path)
        print(json.dumps({"success": False, "error": f"Input file not found: {path}"}))
        sys.exit(1)
    try:
        if p.suffix == ".csv":
            return pd.read_csv(path)
        elif p.suffix == ".bin":
            try:
                return pd.read_parquet(path)
            except Exception:
                try:
                    return pd.read_csv(path)
                except Exception:
                    pass
        elif p.suffix in (".xlsx", ".xls"):
            return pd.read_excel(path)
        else:
            logger.error("Unsupported file format: %s", p.suffix)
            print(json.dumps({"success": False, "error": f"Unsupported file format: {p.suffix}"}))
            sys.exit(1)
    except Exception as e:
        logger.error("Failed to read input: %s", e)
        print(json.dumps({"success": False, "error": f"Failed to read input: {e}"}))
        sys.exit(1)


def output_result(result: Dict[str, Any]) -> None:
    print(json.dumps(result))
    sys.exit(0)


def calculate_similarity(str1: str, str2: str) -> float:
    if not str1 or not str2:
        return 0.0
    s1, s2 = str1.lower(), str2.lower()
    if s1 == s2:
        return 1.0
    common = sum(1 for c in s1 if c in s2)
    return (common / max(len(s1), len(s2))) * 0.8


def normalize_attributes(attrs: Any) -> Dict[str, Any]:
    if isinstance(attrs, dict):
        return {
            "title": str(attrs.get("title", "")).lower().strip(),
            "brand": str(attrs.get("brand", "")).lower().strip(),
            "model": str(attrs.get("model", "")).lower().strip(),
            "size": str(attrs.get("size", "")),
            "color": str(attrs.get("color", "")).lower().strip(),
            "pack_count": int(attrs.get("pack_count", 1)),
            "upc": str(attrs.get("upc", "")),
            "price": float(attrs.get("price", 0.0)),
        }
    return {}


def compare_attributes(
    attr1: Any,
    attr2: Any,
    exact_match_threshold: float = 0.9,
    probable_match_threshold: float = 0.7,
    title_similarity_cutoff: float = 0.8,
    model_weight: float = 0.4,
    brand_weight: float = 0.3,
    title_weight: float = 0.2,
) -> Dict[str, Any]:
    if isinstance(attr1, str) and isinstance(attr2, str):
        sim = calculate_similarity(attr1, attr2)
        if sim > title_similarity_cutoff:
            return {
                "is_match": True,
                "decision": "probable_match",
                "confidence": round(sim, 2),
                "reason": f"Title similarity: {sim:.2f}",
                "title_similarity": sim,
            }
        return {
            "is_match": False,
            "decision": "not_match",
            "confidence": round(sim, 2),
            "reason": f"Title similarity too low: {sim:.2f}",
            "title_similarity": sim,
        }
    if not attr1 or not attr2:
        return {"is_match": False, "confidence": 0.0, "reason": "Missing attributes"}
    n1 = normalize_attributes(attr1)
    n2 = normalize_attributes(attr2)
    results: Dict[str, Any] = {}
    parts: List[float] = []
    if n1.get("model") and n2.get("model"):
        if n1["model"] == n2["model"]:
            results["model_match"] = True
            parts.append(model_weight)
        else:
            results["model_match"] = False
            return {"is_match": False, "confidence": 0.2, "reason": "Model mismatch", **results}
    if n1.get("brand") and n2.get("brand"):
        if n1["brand"] == n2["brand"]:
            results["brand_match"] = True
            parts.append(brand_weight)
        else:
            results["brand_match"] = False
            return {"is_match": False, "confidence": 0.3, "reason": "Brand mismatch", **results}
    title_sim = calculate_similarity(n1.get("title", ""), n2.get("title", ""))
    results["title_similarity"] = title_sim
    if title_sim > title_similarity_cutoff:
        parts.append(title_weight)
    variant_reasons = []
    if n1.get("color") != n2.get("color"):
        variant_reasons.append(f"Color: {n1.get('color')} vs {n2.get('color')}")
        results["color_mismatch"] = True
    if n1.get("size") != n2.get("size"):
        variant_reasons.append(f"Size: {n1.get('size')} vs {n2.get('size')}")
        results["size_mismatch"] = True
    if n1.get("pack_count") != n2.get("pack_count"):
        variant_reasons.append(f"Pack: {n1.get('pack_count')} vs {n2.get('pack_count')}")
        results["pack_mismatch"] = True
    confidence = sum(parts)
    if confidence >= exact_match_threshold:
        decision = "exact_match"
    elif confidence >= probable_match_threshold:
        decision = "probable_match"
    elif variant_reasons:
        decision = "variant_match"
    else:
        decision = "not_match"
    reason_parts = [f"Confidence: {confidence:.2f}"]
    if variant_reasons:
        reason_parts.append("Variant diffs: " + "; ".join(variant_reasons))
    if results.get("title_similarity"):
        reason_parts.append(f"Title sim: {results['title_similarity']:.2f}")
    results["is_match"] = decision == "exact_match"
    results["decision"] = decision
    results["confidence"] = round(confidence, 2)
    results["reason"] = ". ".join(reason_parts)
    return results


def main() -> None:
    input_data = load_job_input()
    payload = input_data.get("payload", {})
    input_path = payload.get("input_path")
    if not input_path:
        output_result({"success": False, "error": "Missing required payload field: input_path"})

    url_column1: str = payload.get("url_column1", "walmart_url")
    url_column2: str = payload.get("url_column2", "comparison_url")
    columns_to_compare: List[str] = payload.get("columns_to_compare", [url_column1, url_column2])
    attributes_column1: Optional[str] = payload.get("attributes_column1")
    attributes_column2: Optional[str] = payload.get("attributes_column2")
    compare_method: str = payload.get("compare_method", "similarity")
    exact_match_threshold: float = float(payload.get("exact_match_threshold", 0.9))
    probable_match_threshold: float = float(payload.get("probable_match_threshold", 0.7))
    title_similarity_cutoff: float = float(payload.get("title_similarity_cutoff", 0.8))
    model_weight: float = float(payload.get("model_weight", 0.4))
    brand_weight: float = float(payload.get("brand_weight", 0.3))
    title_weight: float = float(payload.get("title_weight", 0.2))
    output_path: str = payload.get("output_path") or str(Path(input_path).with_name(f"{Path(input_path).stem}_compared.csv"))

    logger.info("Loading data from %s", input_path)
    df = load_dataframe(input_path)
    logger.info("Loaded %d rows", len(df))

    results: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        comparison: Dict[str, Any] = {}
        if attributes_column1 and attributes_column2:
            try:
                attr1 = json.loads(str(row.get(attributes_column1, "{}")))
            except (json.JSONDecodeError, TypeError):
                attr1 = {}
            try:
                attr2 = json.loads(str(row.get(attributes_column2, "{}")))
            except (json.JSONDecodeError, TypeError):
                attr2 = {}
            if isinstance(attr1, dict) and isinstance(attr2, dict):
                comparison = compare_attributes(attr1, attr2, exact_match_threshold, probable_match_threshold, title_similarity_cutoff, model_weight, brand_weight, title_weight)
            else:
                comparison = {"is_match": False, "decision": "not_match", "confidence": 0.0, "reason": "Invalid attribute data"}
        else:
            val1 = row.get(columns_to_compare[0], "")
            val2 = row.get(columns_to_compare[1], "")
            if isinstance(val1, str) and isinstance(val2, str):
                if compare_method == "exact_match":
                    is_match = val1 == val2
                    comparison = {
                        "is_match": is_match,
                        "decision": "exact_match" if is_match else "not_match",
                        "confidence": 1.0 if is_match else 0.0,
                        "reason": "Exact match" if is_match else "No exact match",
                    }
                else:
                    comparison = compare_attributes(val1, val2, exact_match_threshold, probable_match_threshold, title_similarity_cutoff, model_weight, brand_weight, title_weight)
            elif isinstance(val1, dict) and isinstance(val2, dict):
                comparison = compare_attributes(val1, val2, exact_match_threshold, probable_match_threshold, title_similarity_cutoff, model_weight, brand_weight, title_weight)
            else:
                comparison = {"is_match": False, "decision": "not_match", "confidence": 0.0, "reason": "Incompatible types"}

        entry = row.to_dict()
        entry.update(comparison)
        results.append(entry)

    result_df = pd.DataFrame(results)
    result_df.to_csv(output_path, index=False)
    logger.info("Wrote %d comparison results to %s", len(results), output_path)

    output_result({
        "success": True,
        "data": {
            "processed_rows": len(results),
            "output_path": output_path,
            "metadata": {
                "columns_compared": columns_to_compare,
                "method": compare_method,
                "attribute_mode": bool(attributes_column1 and attributes_column2),
                "exact_matches": sum(1 for r in results if r.get("decision") == "exact_match"),
                "probable_matches": sum(1 for r in results if r.get("decision") == "probable_match"),
                "variant_matches": sum(1 for r in results if r.get("decision") == "variant_match"),
                "no_matches": sum(1 for r in results if r.get("decision") == "not_match"),
            },
        },
        "error": None,
    })


if __name__ == "__main__":
    main()
