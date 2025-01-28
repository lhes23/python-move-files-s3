import boto3
import re

# Initialize S3 client
s3 = boto3.client('s3')

# Define your bucket name
bucket_name = "ippei-com-media-backup"

# Prefix for testing a single folder
prefix = "wp-content/uploads/2025/01/"  # Target a specific folder for testing

def move_files(bucket_name, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    operation_count = 0

    # Use paginator to handle files in the target folder
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                old_key = obj['Key']
                
                # Skip if the key doesn't match the folder structure
                match = re.match(rf"({prefix})\d+/([^/]+)", old_key)
                if not match:
                    continue
                
                # Extract year/month and file name
                new_key = f"{match.group(1)}{match.group(2)}"
                
                # Log the move operation
                print(f"Moving {old_key} to {new_key}")
                
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': old_key},
                    Key=new_key
                )
                s3.delete_object(Bucket=bucket_name, Key=old_key)
                
                operation_count += 1

    print(f"Total files that would be moved: {operation_count}")

# Run the script for testing
move_files(bucket_name, prefix)
