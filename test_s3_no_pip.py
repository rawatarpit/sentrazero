#!/usr/bin/env python3
"""
Supabase Storage S3 Test - Standard Library Only
No pip install needed!
"""

import sys
import hmac
import hashlib
import base64
import datetime
import urllib.request
import urllib.parse
import urllib.error

def sign(secret_key, date_stamp, region, service, string_to_sign):
    """Generate AWS Signature V4"""
    # AWS4-HMAC-SHA256
    k_date = hmac.new(("AWS4" + secret_key).encode(), date_stamp.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def make_request(endpoint, method, access_key, secret_key, path=b"/"):
    """Make signed S3 request"""
    region = "ap-south-1"
    service = "s3"
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    
    host = endpoint.replace("https://", "").replace("http://", "")
    
    # Canonical query string
    canonical_querystring = ""
    
    # Canonical headers
    canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
    
    # Signed headers
    signed_headers = "host;x-amz-date"
    
    # Canonical request
    canonical_request = f"{method}\n{path}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n"
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_request += payload_hash
    
    # String to sign
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    
    # Calculate signature
    signature = sign(secret_key, date_stamp, region, service, string_to_sign)
    
    # Authorization header
    authorization = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    
    url = f"{endpoint}{path}"
    
    req = urllib.request.Request(url, method=method)
    req.add_header("Host", host)
    req.add_header("x-amz-date", amz_date)
    req.add_header("Authorization", authorization)
    
    try:
        response = urllib.request.urlopen(req, timeout=30)
        return response.read().decode(), response.status
    except urllib.error.HTTPError as e:
        return e.read().decode() if e.fp else str(e), e.code
    except Exception as e:
        return str(e), 0

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 test_s3_no_pip.py <access_key> <secret_key>")
        print("\nGet your S3 keys from:")
        print("  Supabase Dashboard → Storage → Settings → S3 Configuration")
        sys.exit(1)
    
    access_key = sys.argv[1]
    secret_key = sys.argv[2]
    
    project_ref = "pqcwgkqrblugplpcaxcy"
    endpoints = [
        f"https://{project_ref}.storage.supabase.co",
        f"https://{project_ref}.storage.supabase.co/storage/v1/s3",
    ]
    
    for endpoint in endpoints:
        print(f"\n{'='*60}")
        print(f"Testing: {endpoint}")
        print(f"{'='*60}")
        
        # List buckets
        print("\n[1] List buckets (GET /)...")
        body, status = make_request(endpoint, "GET", access_key, secret_key, "/")
        print(f"    Status: {status}")
        if status == 200:
            print(f"    Response: {body[:500]}...")
        else:
            print(f"    Error: {body}")
        
        # List objects in datasets
        print("\n[2] List objects in datasets (GET /datasets/)...")
        body, status = make_request(endpoint, "GET", access_key, secret_key, "/datasets/")
        print(f"    Status: {status}")
        if status == 200:
            print(f"    Response: {body[:500]}...")
        else:
            print(f"    Error: {body}")
    
    print(f"\n{'='*60}")
    print("Test complete")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()