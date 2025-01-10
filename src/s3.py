import boto3
from pathlib import Path
from os import path, makedirs

# Initialize an S3 connection
def initialize_s3():
    return boto3.Session().resource('s3')

# Download everything from S3 bucket
def s3_download_all(target_s3_bucket, destination_folder):
    if not target_s3_bucket: return

    print()
    print("* Downloading all files from", target_s3_bucket, "bucket")

    # connect to S3 bucket
    s3 = initialize_s3()
    bucket = s3.Bucket(target_s3_bucket)

    # Loop through all objects in the bucket
    for file in bucket.objects.all():
        destination_path = path.join(destination_folder, file.key)
        makedirs(path.dirname(destination_path), exist_ok=True)
        bucket.download_file(file.key, destination_path)
        print("**", file.key, "downloaded")

    print("** Download is complete")

# Upload specific directories to S3
def s3_upload_specific_folders(target_s3_bucket, folders_list, destination_path_in_s3=""):
    if not target_s3_bucket: return
    if not folders_list: return

    # connect to S3 bucket
    s3 = initialize_s3()
    bucket = s3.Bucket(target_s3_bucket)

    print()
    print("* Uploading folders to", target_s3_bucket, "bucket")

    if destination_path_in_s3 != "":
        destination_path_in_s3 = destination_path_in_s3 + "/"

    for folder_path in folders_list:
        folder = Path(folder_path)  # Specify each folder
        if folder.is_dir():
            for file_path in folder.glob("**/*"):  # Recursively find all files in the folder
                if file_path.is_file():
                    # Determine the key for the S3 object (relative path within the folder)
                    key = destination_path_in_s3 + str(file_path.relative_to(folder.parent))
                    # Upload the file to S3
                    bucket.upload_file(str(file_path), key)
                    print("**", key, "uploaded")
        else:
            print(f"! Error: {folder} is not a valid directory")

    print("** Upload is complete")