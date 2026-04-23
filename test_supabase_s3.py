#!/usr/bin/env python3
"""
Supabase Storage S3 Test using minio library
Install: pip install minio

Usage:
  python3 test_supabase_s3.py <access_key> <secret_key>
"""

import sys
from datetime import datetime
import hashlib
import hmac
import base64

# Check if minio is installed
try:
    from minio import Minio
except ImportError:
    print("Installing minio library...")
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "minio"], check=True)
    from minio import Minio

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 test_supabase_s3.py <access_key> <secret_key>")
        print("\nGet your S3 credentials from:")
        print("  Supabase Dashboard → Storage → Settings → S3 Configuration → Access keys")
        sys.exit(1)
    
    access_key = sys.argv[1]
    secret_key = sys.argv[2]
    
    project_ref = "pqcwgkqrblugplpcaxcy"
    region = "ap-south-1"
    bucket = "datasets"
    
    # Test endpoints - try both formats
    endpoints = [
        # Without /storage/v1/s3
        f"{project_ref}.storage.supabase.co",
        # With /storage/v1/s3
        f"{project_ref}.storage.supabase.co/storage/v1/s3",
    ]
    
    success = False
    
    for endpoint in endpoints:
        print(f"\n{'='*60}")
        print(f"Testing endpoint: https://{endpoint}")
        print(f"{'='*60}")
        
        try:
            client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                region=region
            )
            
            # Test 1: List buckets
            print("\n[1] Listing buckets...")
            buckets = client.list_buckets()
            if buckets:
                for b in buckets:
                    print(f"  ✓ Bucket: {b.name}")
            else:
                print("  (no buckets found or empty)")
            
            # Test 2: List objects in datasets bucket
            print(f"\n[2] Listing objects in '{bucket}' bucket...")
            try:
                objects = list(client.list_objects(bucket, recursive=True))
                if objects:
                    for o in objects[:10]:  # Show first 10
                        print(f"  ✓ {o.object_name} ({o.size} bytes)")
                    if len(objects) > 10:
                        print(f"  ... and {len(objects) - 10} more")
                else:
                    print("  (no objects found)")
            except Exception as e:
                print(f"  ✗ Error: {e}")
            
            success = True
            print(f"\n✓ SUCCESS with https://{endpoint}")
            
        except Exception as e:
            print(f"✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
    
    if success:
        print(f"\n{'='*60}")
        print("At least one endpoint works!")
        print(f"{'='*60}")
    else:
        print(f"\n✗ All endpoints failed")
        sys.exit(1)

if __name__ == "__main__":
    main()