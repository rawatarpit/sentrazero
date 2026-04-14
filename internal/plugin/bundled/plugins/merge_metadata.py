#!/usr/bin/env python3
"""
Bundled Merge Metadata Plugin
Merges multiple datasets/chunks for RISEOTB operations
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def merge_datasets(datasets: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dataset metadata records"""
    result = {
        "merged": True,
        "dataset_count": len(datasets),
        "total_rows": 0,
        "total_size_bytes": 0,
        "columns": set(),
        "merged_columns": [],
        "output_path": config.get("output_path", ""),
    }

    for ds in datasets:
        result["total_rows"] += ds.get("row_count", 0)
        result["total_size_bytes"] += ds.get("size_bytes", 0)

        if "columns" in ds:
            result["columns"].update(ds["columns"])

    result["merged_columns"] = sorted(list(result["columns"]))

    return result


def merge_chunk_results(result_files: List[str], config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge results from multiple chunk processing outputs"""
    all_results = []

    for result_file in result_files:
        try:
            with open(result_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_results.extend(data)
                else:
                    all_results.append(data)
        except Exception:
            continue

    merged = {
        "chunks_processed": len(result_files),
        "total_records": len(all_results),
        "results": all_results,
    }

    return merged


def main():
    """Main entry point"""
    input_data = json.loads(sys.stdin.read() if not sys.stdin.isatty() else "{}")

    datasets = input_data.get("datasets", [])
    result_files = input_data.get("result_files", [])
    config = input_data.get("config", {})

    if result_files:
        result = merge_chunk_results(result_files, config)
    elif datasets:
        result = merge_datasets(datasets, config)
    else:
        result = {
            "merged": False,
            "error": "No datasets or result_files provided"
        }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()