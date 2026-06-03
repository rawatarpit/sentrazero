#!/usr/bin/env python3
import sys
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("dedup")

try:
    from rapidfuzz import fuzz
except ImportError:
    from difflib import SequenceMatcher

    class _Fuzz:
        @staticmethod
        def token_sort_ratio(s1: str, s2: str) -> float:
            return SequenceMatcher(None, s1, s2).ratio() * 100

        @staticmethod
        def ratio(s1: str, s2: str) -> float:
            return SequenceMatcher(None, s1, s2).ratio() * 100

    fuzz = _Fuzz()


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


def clean_string(text: Any) -> str:
    return " ".join(str(text).lower().strip().split())


def compute_similarity(row1: pd.Series, row2: pd.Series, match_fields: List[str]) -> float:
    scores: List[float] = []
    for field in match_fields:
        if field not in row1.index or field not in row2.index:
            continue
        v1 = clean_string(row1[field])
        v2 = clean_string(row2[field])
        if v1 and v2:
            try:
                scores.append(fuzz.token_sort_ratio(v1, v2) / 100.0)
            except Exception:
                scores.append(fuzz.ratio(v1, v2) / 100.0)
    return sum(scores) / len(scores) if scores else 0.0


def detect_variant(
    row1: pd.Series,
    row2: pd.Series,
    variant_fields: Optional[List[str]] = None,
    title_similarity_threshold: float = 0.8,
) -> Tuple[bool, str, str]:
    brand1 = str(row1.get("brand", "")).lower()
    brand2 = str(row2.get("brand", "")).lower()
    if not (brand1 and brand2 and brand1 == brand2):
        return False, "not_variant", ""
    t1 = str(row1.get("title", "")).lower()
    t2 = str(row2.get("title", "")).lower()
    try:
        sim = fuzz.token_sort_ratio(t1, t2) / 100.0
    except Exception:
        sim = fuzz.ratio(t1, t2) / 100.0
    if sim <= title_similarity_threshold:
        return False, "not_variant", ""
    fields = variant_fields or ["color", "size", "pack"]
    diffs = []
    for attr in fields:
        v1 = str(row1.get(attr, "")).lower()
        v2 = str(row2.get(attr, "")).lower()
        if v1 and v2 and v1 != v2:
            diffs.append(f"{attr}: {v1} vs {v2}")
    if not diffs:
        return False, "not_variant", ""
    return True, "variant_match", "; ".join(diffs)


def find_duplicates(
    df: pd.DataFrame,
    match_fields: List[str],
    threshold: float,
    id_column: str = "product_id",
    variant_fields: Optional[List[str]] = None,
    variant_title_threshold: float = 0.8,
) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
    pairs: List[Dict[str, Any]] = []
    df = df.reset_index(drop=True)
    n = len(df)
    seen: set = set()
    for i in range(n):
        if i in seen:
            continue
        for j in range(i + 1, n):
            if j in seen:
                continue
            sim = compute_similarity(df.iloc[i], df.iloc[j], match_fields)
            if sim >= threshold:
                pid_a = str(df.iloc[i].get(id_column, f"row_{i}"))
                pid_b = str(df.iloc[j].get(id_column, f"row_{j}"))
                is_var, var_type, var_reason = detect_variant(df.iloc[i], df.iloc[j], variant_fields, variant_title_threshold)
                pairs.append({
                    "product_id_a": pid_a,
                    "product_id_b": pid_b,
                    "similarity": round(sim, 3),
                    "type": var_type,
                    "reason": var_reason,
                })
                seen.add(j)
    unique = df[~df.index.isin(seen)]
    return pairs, unique


def main() -> None:
    input_data = load_job_input()
    payload = input_data.get("payload", {})
    input_path = payload.get("input_path")
    if not input_path:
        output_result({"success": False, "error": "Missing required payload field: input_path"})

    match_fields: List[str] = payload.get("match_fields", ["title", "brand", "upc"])
    threshold: float = float(payload.get("similarity_threshold", 0.9))
    id_column: str = payload.get("id_column", "product_id")
    variant_fields: Optional[List[str]] = payload.get("variant_fields")
    variant_title_threshold: float = float(payload.get("variant_title_threshold", 0.8))
    field_mapping: Dict[str, str] = payload.get("field_mapping", {})
    pairs_output_path: Optional[str] = payload.get("pairs_output_path")
    output_path: str = payload.get("output_path") or str(Path(input_path).with_name(f"{Path(input_path).stem}_deduped.csv"))

    logger.info("Loading data from %s", input_path)
    df = load_dataframe(input_path)
    logger.info("Loaded %d rows, deduplicating on %s (threshold=%.2f)", len(df), match_fields, threshold)

    if field_mapping:
        for old_name, new_name in field_mapping.items():
            if old_name in df.columns and new_name not in df.columns:
                df[new_name] = df[old_name]
                logger.info("Mapped column '%s' -> '%s' for dedup matching", old_name, new_name)

    duplicates, unique_df = find_duplicates(df, match_fields, threshold, id_column, variant_fields, variant_title_threshold)
    unique_df.to_csv(output_path, index=False)
    logger.info("Found %d duplicates, wrote %d unique rows to %s", len(duplicates), len(unique_df), output_path)

    if pairs_output_path and duplicates:
        pairs_df = pd.DataFrame(duplicates)
        pairs_df.to_csv(pairs_output_path, index=False)
        logger.info("Wrote %d duplicate pair details to %s", len(duplicates), pairs_output_path)

    avg_sim = round(sum(d["similarity"] for d in duplicates) / len(duplicates), 2) if duplicates else 0.0
    output_result({
        "success": True,
        "data": {
            "processed_rows": len(df),
            "output_path": output_path,
            "metadata": {
                "duplicate_count": len(duplicates),
                "unique_count": len(unique_df),
                "match_fields": match_fields,
                "similarity_threshold": threshold,
                "avg_similarity": avg_sim,
            },
        },
        "error": None,
    })


if __name__ == "__main__":
    main()
