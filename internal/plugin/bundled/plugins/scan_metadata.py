#!/usr/bin/env python3
"""
Bundled Scan Metadata Plugin
Extracts metadata from datasets for scan operations
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def detect_file_type(filename: str) -> str:
    """Detect file type from extension"""
    ext = Path(filename).suffix.lower()
    type_map = {
        '.csv': 'csv',
        '.json': 'json',
        '.jsonl': 'jsonl',
        '.parquet': 'parquet',
        '.txt': 'text',
        '.tsv': 'tsv',
        '.xlsx': 'excel',
        '.xls': 'excel',
        '.xml': 'xml',
        '.yaml': 'yaml',
        '.yml': 'yaml',
    }
    return type_map.get(ext, 'unknown')


def scan_directory(path: str) -> Dict[str, Any]:
    """Scan directory and extract metadata"""
    result = {
        "file_count": 0,
        "total_size_bytes": 0,
        "file_types": {},
        "headers": [],
        "columns": [],
        "sample_files": [],
        "largest_file": None,
        "smallest_file": None,
    }
    
    if not os.path.exists(path):
        return result
    
    path_obj = Path(path)
    
    if path_obj.is_file():
        files = [path_obj]
    else:
        files = list(path_obj.rglob('*'))
        files = [f for f in files if f.is_file()]
    
    for f in files:
        result["file_count"] += 1
        try:
            size = f.stat().st_size
            result["total_size_bytes"] += size
            
            file_type = detect_file_type(str(f))
            result["file_types"][file_type] = result["file_types"].get(file_type, 0) + 1
            
            if result.get("largest_file") is None or size > result["largest_file"]["size"]:
                result["largest_file"] = {"name": str(f), "size": size}
            
            if result.get("smallest_file") is None or size < result["smallest_file"]["size"]:
                result["smallest_file"] = {"name": str(f), "size": size}
            
            if len(result["sample_files"]) < 5:
                result["sample_files"].append(str(f))
                
        except Exception:
            continue
    
    return result


def detect_csv_headers(filepath: str) -> List[str]:
    """Detect headers from CSV file"""
    try:
        import csv
        with open(filepath, 'r', newline='', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if headers:
                return [h.strip() for h in headers if h.strip()]
    except Exception:
        pass
    return []


def detect_json_structure(filepath: str) -> Dict[str, Any]:
    """Detect structure from JSON file"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            data = json.load(f)
            if isinstance(data, list) and len(data) > 0:
                if isinstance(data[0], dict):
                    return {"type": "array_of_objects", "keys": list(data[0].keys())}
            elif isinstance(data, dict):
                return {"type": "object", "keys": list(data.keys())}
    except Exception:
        pass
    return {}


def main():
    """Main entry point"""
    input_data = json.loads(sys.stdin.read() if not sys.stdin.isatty() else "{}")
    
    input_path = input_data.get("input_path", "")
    if not input_path:
        input_path = os.getcwd()
    
    result = scan_directory(input_path)
    
    sample_files = result.get("sample_files", [])
    if sample_files:
        headers = detect_csv_headers(sample_files[0])
        if headers:
            result["headers"] = headers
            result["columns"] = headers
        
        json_struct = detect_json_structure(sample_files[0])
        if json_struct:
            result["json_structure"] = json_struct
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
