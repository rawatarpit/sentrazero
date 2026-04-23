# AWS CLI Installation

## On Linux/macOS

```bash
# Download
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

# Unzip
unzip -q awscliv2.zip

# Install
sudo ./aws/install

# Verify
aws --version
```

## On Ubuntu/Linux (alternative)

```bash
# Install pip first
sudo apt update
sudo apt install python3-pip

# Install awscli
pip3 install awscli

# Or awscli-local
pip3 install awscli-local
```

## On macOS

```bash
# Using Homebrew
brew install awscli

# Or
pip3 install awscli
```

## After Installation

```bash
# Configure
aws configure
# Enter your details:
#   AWS Access Key ID: [your access key]
#   AWS Secret Access Key: [your secret key]
#   Default region name: ap-south-1
#   Default output format: json

# Test
aws s3 ls --endpoint-url https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3

# List objects in datasets bucket
aws s3 ls s3://datasets/ --endpoint-url https://pqcwgkqrblugplpcaxcy.storage.supabase.co/storage/v1/s3
```

## Quick Install Script

```bash
# One-liner for Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip && unzip -q -o /tmp/awscliv2.zip -d /tmp && sudo /tmp/aws/install && rm -rf /tmp/aws /tmp/awscliv2.zip && aws --version
```