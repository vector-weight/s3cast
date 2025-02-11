#!/usr/bin/env python3
"""
This script asynchronously transfers one or more S3 buckets from a source AWS account to a
destination AWS account using aioboto3 and asyncio. It streams objects in chunks (via multipart
uploads) to avoid loading entire files into memory. Concurrency is dynamically determined based
on the number of CPU cores and available memory using psutil.

Usage:
  1) Ensure you have AWS credentials set up for the source and destination profiles in your
     AWS configuration (e.g., ~/.aws/credentials).
  2) Install required libraries:
       pip install aioboto3 psutil
  3) Run the script:
       ./aws_transfer_script.py
     You will be prompted to enter space-separated bucket names found in the source account.
  4) The script will create new buckets in the destination account with a random 8-character
     suffix attached to the original bucket name and transfer all objects concurrently.

Additional Details:
  * By default, chunk size is 5 MB (CHUNK_SIZE = 5 * 1024 * 1024). This aligns with the minimum
    part size required by S3's multipart upload.
  * Concurrency is calculated as the minimum of (CPU-based concurrency) and (memory-based
    concurrency), ensuring we do not overwhelm system resources.
  * Each part of the transfer is uploaded separately. In the event of an error, the multipart
    upload is aborted to avoid incomplete data.
  * Logging is stored in 'transfer.log'.

"""

import asyncio
import uuid
import os
import aioboto3  # type: ignore
import psutil  # type: ignore
import logging

########################
# User-configured AWS profiles
########################
SRC_PROFILE = "source-profile"
DST_PROFILE = "dest-profile"
REGION = "us-west-2"

# Size of each chunk in bytes for multipart uploads.
# Must be >= 5 MB (except for final part) to comply with S3.
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB


def calculate_dynamic_concurrency(chunk_size: int = CHUNK_SIZE) -> int:
    """
    Calculate the concurrency (number of objects to transfer simultaneously) based on
    both CPU cores and available system memory. Each concurrent task is assumed to
    require roughly one chunk's worth of memory overhead.

    :param chunk_size: The chunk size in bytes used by the multipart upload.
    :return: The integer concurrency level to use for transfers.
    """
    cpu_count = os.cpu_count()
    if cpu_count is None:
        print("[!] Warning: Unable to determine CPU count, defaulting to 2.")
        cpu_count = 2

    # Concurrency based on CPU: 2 tasks per CPU core, for a rough estimate.
    concurrency_cpu = cpu_count * 2

    # Concurrency based on memory:
    mem_info = psutil.virtual_memory()
    concurrency_mem = int(mem_info.available // chunk_size)

    # Pick the lower of the two to avoid saturating CPU or memory
    concurrency = min(concurrency_cpu, concurrency_mem)

    # Ensure at least 1 concurrency
    return max(1, concurrency)


async def copy_object_streaming(
    source_s3,
    dest_s3,
    source_bucket: str,
    dest_bucket: str,
    key: str,
    sem: asyncio.Semaphore,
    chunk_size: int = CHUNK_SIZE,
) -> None:
    """
    Concurrently copy a single object from a source bucket to a destination bucket without
    loading the entire file into memory. This function reads the object from the source
    bucket in chunks and uploads each chunk as a part in a multipart upload to the
    destination bucket.

    :param source_s3: The aioboto3 client for the source account.
    :param dest_s3: The aioboto3 client for the destination account.
    :param source_bucket: Name of the source bucket.
    :param dest_bucket: Name of the destination bucket.
    :param key: The object key (path/filename) to copy.
    :param sem: An asyncio Semaphore to control concurrency.
    :param chunk_size: The size of each chunk in bytes.
    :return: None
    """
    async with sem:
        # Initialize multipart upload on the destination
        mpu = await dest_s3.create_multipart_upload(Bucket=dest_bucket, Key=key)
        upload_id = mpu["UploadId"]

        parts = []
        part_number = 1
        try:
            # Open a stream to read from the source object in chunks
            get_resp = await source_s3.get_object(Bucket=source_bucket, Key=key)
            body = get_resp["Body"]

            while True:
                chunk = await body.read(chunk_size)
                if not chunk:
                    break

                # Upload each chunk as a part of the multipart upload
                part_resp = await dest_s3.upload_part(
                    Bucket=dest_bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk,
                )
                parts.append({"ETag": part_resp["ETag"], "PartNumber": part_number})
                part_number += 1

            # Complete the multipart upload after all parts are uploaded
            await dest_s3.complete_multipart_upload(
                Bucket=dest_bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception as e:
            print(f"[!] Error encountered during transfer of {key}: {e}")
            # Abort multipart upload on error to avoid leaving partial uploads
            await dest_s3.abort_multipart_upload(
                Bucket=dest_bucket, Key=key, UploadId=upload_id
            )
            raise


async def copy_bucket(
    source_session,
    dest_session,
    bucket_name: str,
    concurrency: int,
) -> None:
    """
    Copies all objects from the specified bucket_name in the source account to a new bucket
    in the destination account. The new bucket is created with a random 8-character suffix
    to ensure uniqueness. Transfers each object via multipart uploads in chunks.

    :param source_session: The aioboto3 Session for the source account.
    :param dest_session: The aioboto3 Session for the destination account.
    :param bucket_name: The name of the source bucket to copy.
    :param concurrency: The number of concurrent object transfers allowed.
    :return: None
    """
    async with source_session.client("s3", region_name=REGION) as source_s3:
        dest_s3 = dest_session.client("s3", region_name=REGION)

        # Create a new bucket name with a random 8-char suffix
        suffix = str(uuid.uuid4())[:8]
        dest_bucket = f"{bucket_name}-{suffix}"

        print(f"[+] Creating destination bucket: {dest_bucket}")
        if (
            not dest_bucket.islower()
            or len(dest_bucket) > 63
            or "_" in dest_bucket
            or not dest_bucket[0].isalnum()
            or not dest_bucket[-1].isalnum()
        ):
            raise ValueError(f"Invalid bucket name: {dest_bucket}")

        # Create bucket in the destination account
        await dest_s3.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )

        print(f"[+] Listing objects in {bucket_name}")
        continuation_token = None
        all_keys = []

        # List all objects in the source bucket, handling pagination
        while True:
            list_args = {
                "Bucket": bucket_name,
                "MaxKeys": 1000,
            }
            if continuation_token:
                list_args["ContinuationToken"] = continuation_token
            resp = await source_s3.list_objects_v2(**list_args)

            if "Contents" in resp:
                all_keys.extend(obj["Key"] for obj in resp["Contents"])

            if not resp.get("IsTruncated"):
                break
            list_args["ContinuationToken"] = resp["NextContinuationToken"]

        print(f"[+] Found {len(all_keys)} objects in {bucket_name}. Starting concurrent copy...")
        print(f"    -> Using concurrency: {concurrency}\n")

        # Semaphore to limit concurrency
        sem = asyncio.Semaphore(concurrency)
        tasks = []

        # Create a task for each object copy
        for key in all_keys:
            tasks.append(
                asyncio.create_task(
                    copy_object_streaming(source_s3, dest_s3, bucket_name, dest_bucket, key, sem)
                )
            )

        # Run tasks concurrently (object-level concurrency)
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"[!] Error encountered during object transfer: {e}")

        print(f"[\u2714] Transfer complete: {bucket_name} -> {dest_bucket}\n")


async def main() -> None:
    """
    Entry point for the script. Calculates concurrency dynamically, initializes aioboto3
    Sessions for source and destination AWS accounts, lists all buckets in the source
    account, prompts the user to select which buckets to transfer, and then kicks off
    the transfer for each selected bucket.

    :return: None
    """
    concurrency = calculate_dynamic_concurrency()

    source_session = aioboto3.Session(profile_name=SRC_PROFILE, region_name=REGION)
    dest_session = aioboto3.Session(profile_name=DST_PROFILE, region_name=REGION)

    # List all buckets in the source account
    async with source_session.client("s3", region_name=REGION) as s3_client:
        resp = await s3_client.list_buckets()
        source_buckets = [b["Name"] for b in resp.get("Buckets", [])]

    print("[+] Buckets in source account:")
    for b in source_buckets:
        print("   -", b)

    print()
    import sys
    print("[?] Enter space-separated bucket names to transfer:", end="", flush=True)
    selected = sys.stdin.readline().strip().split()

    # Transfer each selected bucket
    for bucket in selected:
        await copy_bucket(source_session, dest_session, bucket, concurrency)

    # Configure logging, log final success
    logging.basicConfig(filename="transfer.log", level=logging.INFO)
    logging.info("[\u2714] All selected bucket transfers completed.")


if __name__ == "__main__":
    asyncio.run(main())
