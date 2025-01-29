import boto3
import re

# Initialize S3 client
s3 = boto3.client('s3')

# Define your bucket name
bucket_name = "ippei-com-media-backup"

# Prefix for testing a single folder
prefix = "wp-content/uploads/2023/"  # Target a specific folder for testing

def move_files(bucket_name, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    operation_count = 0
    skipped_count = 0

    # Use paginator to handle files in the target folder
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                old_key = obj['Key']
                
                # Match files inside a version-number subfolder
                match = re.match(rf"({prefix}\d{{2}})/\d+/([^/]+)", old_key)
                if not match:
                    continue
                
                # Extract new target path without the version number
                new_key = f"{match.group(1)}/{match.group(2)}"

                # Check if the file already exists in the new location
                existing_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=new_key)
                if 'Contents' in existing_objects:
                    print(f"Skipping {old_key}, {new_key} already exists.")
                    skipped_count += 1
                    continue

                # Log the move operation
                print(f"Moving {old_key} to {new_key}")

                # Copy the file to the new location
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': old_key},
                    Key=new_key,
                    MetadataDirective="COPY"
                )

                # Delete the original file after successful copy
                s3.delete_object(Bucket=bucket_name, Key=old_key)
                
                operation_count += 1

    print(f"Total files moved: {operation_count}")
    print(f"Total files skipped (already exist): {skipped_count}")

move_files(bucket_name, prefix)