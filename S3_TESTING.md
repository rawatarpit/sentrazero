# Supabase Storage S3 Testing Guide

## Quick Test - AWS CLI

### Step 1: Install AWS CLI v2

#### On Mac (M1/Intel)

```bash
# Option 1: Download and open the installer
curl -L "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o aws.pkg
open aws.pkg
# Enter your password when prompted
```

#### On Linux

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
sudo ./aws/install
rm -rf aws awscliv2.zip
```

#### Using pip (any OS)

```bash
pip3 install awscli
```

### Step 2: Configure AWS CLI

```bash
aws configure
```

Enter these values:
- **AWS Access Key ID**: Copy from Supabase Dashboard → Storage → Settings → S3 Configuration → Access keys
- **AWS Secret Access Key**: Copy from Supabase Dashboard → Storage → Settings → S3 Configuration → Access keys
- **Default region name**: `ap-south-1` (or your project's region)
- **Default output format**: `json`

### Step 3: Test S3 Connection

#### List All Buckets

```bash
aws s3 ls --endpoint-url https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3
```

#### List Objects in datasets Bucket

```bash
aws s3 ls s3://datasets/ --endpoint-url https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3
```

#### Download a File

```bash
aws s3 cp s3://datasets/test-data.csv . --endpoint-url https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3
```

#### Upload a Test File

```bash
echo "test content" > testfile.txt
aws s3 cp testfile.txt s3://datasets/testfile.txt --endpoint-url https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3
```

---

## Your Configuration

| Setting | Value |
|---------|-------|
| Endpoint | `https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3` |
| Region | `ap-south-1` |
| Bucket | `datasets` |
| Access Key | Get from Supabase Dashboard → Storage → Settings → S3 Configuration → Access keys |
| Secret Key | Get from Supabase Dashboard → Storage → Settings → S3 Configuration → Access keys |

---

## Expected Output

### Successful List Buckets

```
2024-01-15 10:30:00 datasets
```

### Successful List Objects

```
2024-01-15 10:30:00      1024 test-data.csv
2024-01-15 10:30:00      2048 another-file.csv
```

---

## Common Errors & Fixes

### "The specified bucket does not exist"

- Check bucket name is correct: `datasets` (not `storage/datasets`)
- Make sure S3 protocol is enabled in Storage → Settings → S3 Configuration

### "SignatureDoesNotMatch"

- Your access key or secret key is incorrect
- Date/time is wrong on your machine
- Regenerate keys in Supabase Dashboard

### "Access Denied"

- S3 protocol is not enabled
- Go to Storage → Settings → S3 Configuration → Enable S3

### "Unable to locate credentials"

- Run `aws configure` again with correct credentials

---

## Test Without AWS CLI (curl)

```bash
# List buckets (no authentication for public buckets)
curl "https://pqcwgkqrblugplpcaxcy.storage.supabase.co/"

# List objects in datasets (public bucket)
curl "https://pqcwgkqrblugplpcaxcy.storage.supabase.co/datasets/"

# Download specific file
curl "https://pqcwgkqrblugplpcaxcy.storage.supabase.co/datasets/test-data.csv" -o test-data.csv
```

---

## Next Steps After Testing

Once S3 works via AWS CLI:
1. Note which endpoint format works (with or without `/storage/v1/s3`)
2. Go back to your agent code
3. Update the endpoint in the Go code if needed
4. Deploy and test the agent again