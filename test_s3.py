#!/usr/bin/env python3
"""
Quick S3 Connectivity Test for Supabase Storage
Run: python3 test_s3.py <access_key> <secret_key>
"""

import sys
from minio import Minio

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 test_s3.py <access_key> <secret_key>")
        sys.exit(1)
    
    access_key = sys.argv[1]
    secret_key = sys.argv[2]
    
    # Try endpoint WITHOUT /storage/v1/s3 path (for minio-go)
    endpoint1 = "pqcwgkqrblugplpcaxcy.storage.supabase.co"
    
    # Try endpoint WITH /storage/v1/s3 path
    endpoint2 = "pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3"
    
    for ep in [endpoint1, endpoint2]:
        print(f"\n{'='*50}")
        print(f"Testing endpoint: {ep}")
        print(f"{'='*50}")
        
        try:
            client = Minio(
                ep,
                access_key=access_key,
                secret_key=secret_key,
                region="ap-south-1"
            )
            
            # List buckets
            print("\nListing buckets...")
            buckets = client.list_buckets()
            for b in buckets:
                print(f"  - {b.name} (created: {b.creation_date})")
            
            # List objects in datasets bucket
            print("\nListing objects in 'datasets' bucket...")
            objects = client.list_objects("datasets", recursive=True)
            for o in objects:
                print(f"  - {o.object_name} ({o.size} bytes)")
            
            print(f"\n✓ SUCCESS with {ep}")
            
        except Exception as e:
            print(f"✗ FAILED: {e}")
    
    print(f"\n{'='*50}")
    print("Test complete")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()