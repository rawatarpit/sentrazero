#!/usr/bin/env python3
import sys
import json
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("coverage")

DEFAULT_PLATFORM_CONFIGS: Dict[str, Dict[str, str]] = {
    "amazon": {"base_url": "https://www.amazon.com", "search_path": "/s", "query_param": "k"},
    "ebay": {"base_url": "https://www.ebay.com", "search_path": "/sch/i.html", "query_param": "_nkw"},
    "flipkart": {"base_url": "https://www.flipkart.com", "search_path": "/search", "query_param": "q"},
}

DEFAULT_USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
DEFAULT_REQUEST_TIMEOUT: int = 30


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
        return pd.read_csv(path)
    except Exception as e:
        logger.error("Failed to read CSV: %s", e)
        print(json.dumps({"success": False, "error": f"Failed to read input: {e}"}))
        sys.exit(1)


def output_result(result: Dict[str, Any]) -> None:
    print(json.dumps(result))
    sys.exit(0)


def build_search_url(platform: str, query: str, configs: Dict[str, Dict[str, str]]) -> Optional[str]:
    cfg = configs.get(platform)
    if not cfg:
        return None
    import urllib.parse
    base = cfg["base_url"].rstrip("/")
    path = cfg.get("search_path", "/")
    param = cfg.get("query_param", "q")
    return f"{base}{path}?{param}={urllib.parse.quote(query[:200])}"


def search_platform(platform: str, query: str, configs: Dict[str, Dict[str, str]], user_agent: str, timeout: int) -> List[Dict[str, Any]]:
    url = build_search_url(platform, query, configs)
    if not url:
        logger.warning("No configuration found for platform: %s", platform)
        return []
    try:
        headers = {"User-Agent": user_agent}
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            logger.warning("Search %s returned status %d", platform, resp.status_code)
            return []
        try:
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception:
            soup = BeautifulSoup(resp.text, "html.parser")
        return _parse_search_results(soup, platform)
    except requests.RequestException as e:
        logger.error("Request failed for %s: %s", platform, e)
        return []


def _parse_search_results(soup: BeautifulSoup, platform: str) -> List[Dict[str, Any]]:
    parsers: Dict[str, Any] = {
        "amazon": _parse_amazon_search,
        "ebay": _parse_ebay_search,
        "flipkart": _parse_flipkart_search,
    }
    parser = parsers.get(platform)
    return parser(soup) if parser else []


def _parse_amazon_search(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    results = []
    for item in soup.find_all("div", {"data-component-type": "s-search-result"})[:5]:
        asin = item.get("data-asin", "")
        title_el = item.find("span", {"class": "a-text-normal"})
        title = title_el.get_text(strip=True)[:200] if title_el else ""
        url = f"https://www.amazon.com/dp/{asin}" if asin else ""
        results.append({"title": title, "url": url})
    return results


def _parse_ebay_search(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    results = []
    for item in soup.select(".s-item__wrapper")[:5]:
        link = item.find("a", {"class": "s-item__link"})
        title_el = item.find("span", {"role": "heading"})
        title = title_el.get_text(strip=True)[:200] if title_el else ""
        url = link.get("href", "") if link else ""
        results.append({"title": title, "url": url})
    return results


def _parse_flipkart_search(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    results = []
    for item in soup.select("._1AtVbE")[:5]:
        link = item.find("a", {"class": "_1fQZEK"})
        title_el = item.find("a", {"class": "_4rR01T"})
        title = title_el.get_text(strip=True)[:200] if title_el else ""
        url = f"https://www.flipkart.com{link.get('href', '')}" if link else ""
        results.append({"title": title, "url": url})
    return results


def main() -> None:
    input_data = load_job_input()
    payload = input_data.get("payload", {})
    input_path = payload.get("input_path")
    if not input_path:
        output_result({"success": False, "error": "Missing required payload field: input_path"})

    walmart_url_col: str = payload.get("walmart_url_column", "walmart_url")
    walmart_attrs_col: str = payload.get("walmart_attributes_column", "walmart_url_attributes_json")
    platforms_col: str = payload.get("platforms_column", "platforms")
    configs: Dict = payload.get("platform_configs", DEFAULT_PLATFORM_CONFIGS)
    user_agent: str = payload.get("user_agent", DEFAULT_USER_AGENT)
    timeout: int = int(payload.get("request_timeout", DEFAULT_REQUEST_TIMEOUT))
    max_results: int = int(payload.get("max_results_per_platform", 3))

    logger.info("Loading data from %s", input_path)
    df = load_dataframe(input_path)
    logger.info("Loaded %d rows", len(df))

    results: List[Dict[str, Any]] = []
    url_columns_checked: set = set()

    for _, row in df.iterrows():
        entry: Dict[str, Any] = row.to_dict()
        walmart_url: str = str(row.get(walmart_url_col, ""))
        walmart_found: bool = bool(walmart_url and walmart_url.startswith("http"))
        entry["walmart_found"] = walmart_found

        title: str = ""
        attrs_json: str = str(row.get(walmart_attrs_col, ""))
        if attrs_json and attrs_json != "{}":
            try:
                attrs = json.loads(attrs_json)
                title = str(attrs.get("title", ""))
            except (json.JSONDecodeError, TypeError):
                pass
        if not title:
            title = str(row.get("product_title", ""))

        platforms_raw: str = str(row.get(platforms_col, ""))
        platforms: List[str] = [p.strip() for p in platforms_raw.split(",") if p.strip()]
        found_count: int = 1 if walmart_found else 0
        platforms_found: List[str] = []

        for platform in platforms:
            if platform == "walmart":
                continue
            url_col: str = f"{platform}_url"
            existing_url: str = str(row.get(url_col, ""))
            existing_url_1: str = str(row.get(f"{platform}_url_1", ""))
            if existing_url and existing_url.startswith("http"):
                entry[url_col] = existing_url
                entry[f"{platform}_found"] = True
                if platform not in platforms_found:
                    platforms_found.append(platform)
                    found_count += 1
                url_columns_checked.add(url_col)
            elif existing_url_1 and existing_url_1.startswith("http"):
                for result_idx in range(1, 4):
                    u = str(row.get(f"{platform}_url_{result_idx}", ""))
                    if u and u.startswith("http"):
                        entry[f"{platform}_url_{result_idx}"] = u
                        entry[f"{platform}_found"] = True
                if f"{platform}_url_1" in entry:
                    entry[url_col] = entry[f"{platform}_url_1"]
                if platform not in platforms_found:
                    platforms_found.append(platform)
                    found_count += 1
                url_columns_checked.add(url_col)
            elif title and platform in configs:
                search_results = search_platform(platform, title, configs, user_agent, timeout)
                if search_results:
                    entry[f"{platform}_found"] = True
                    for idx, hit in enumerate(search_results[:max_results], 1):
                        entry[f"{platform}_url_{idx}"] = hit["url"]
                        entry[f"{platform}_title_{idx}"] = hit["title"]
                    entry[url_col] = search_results[0]["url"]
                    platforms_found.append(platform)
                    found_count += 1
                    url_columns_checked.add(url_col)
                else:
                    entry[f"{platform}_found"] = False
            else:
                entry[f"{platform}_found"] = False

        entry["total_found"] = found_count
        entry["platforms_found"] = ",".join(platforms_found)
        results.append(entry)

    result_df = pd.DataFrame(results)
    output_path: str = payload.get("output_path") or str(Path(input_path).with_name(f"{Path(input_path).stem}_coverage.csv"))
    result_df.to_csv(output_path, index=False)

    total_products: int = len(results)
    walmart_only: int = sum(1 for r in results if r["total_found"] == 1)
    all_found: int = sum(1 for r in results if r["total_found"] > 3)
    total_found: int = sum(r["total_found"] for r in results)

    max_platforms: int = max((len(r.get("platforms_found", "").split(",")) for r in results), default=0)
    total_possible: int = total_products * (max_platforms + 1) if max_platforms > 0 else total_products
    coverage_pct: float = (total_found / total_possible * 100) if total_possible > 0 else 0.0

    output_result({
        "success": True,
        "data": {
            "processed_rows": total_products,
            "output_path": output_path,
            "metadata": {
                "total_products": total_products,
                "url_columns_checked": sorted(url_columns_checked),
                "total_found": total_found,
                "total_possible": total_possible,
                "coverage_percentage": round(coverage_pct, 1),
                "walmart_only_count": walmart_only,
                "all_platforms_found_count": all_found,
            },
        },
        "error": None,
    })


if __name__ == "__main__":
    main()
