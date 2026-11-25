r"""Put one downloaded Alpaca historical SIP file in the project's ADLS folder.

Run from the project root:
    py -3 .\src\ingestion\upload_to_adls.py --file "path\to\AAPL_file.jsonl"

Sign in with `az login` first. No storage key is required by this script.
Existing Azure files are never replaced. Matching sizes do not prove that two
files have identical contents. This script does not compare full-file hashes.
"""

import argparse
import json
import re
import sys
from pathlib import Path
from uuid import uuid4

from azure.core import MatchConditions
from azure.core.exceptions import (
    AzureError,
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ACCOUNT_NAME = "ststockmarketdev261119"
FILE_SYSTEM = "raw"
DEFAULT_FILE = (
    PROJECT_ROOT
    / "data/raw/historical/sip/trades"
    / "AAPL_20261118T121315580619Z.jsonl"
)


def validate_local_file(local_file, symbol):
    """Check the local input before making any Azure requests."""
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", symbol):
        raise ValueError("Use a stock symbol such as AAPL or MSFT.")
    if not local_file.is_file():
        raise ValueError(f"Local file not found: {local_file}. Use --file if needed.")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.jsonl", local_file.name):
        raise ValueError("Expected a .jsonl filename containing letters, numbers, _, . or -.")
    if not local_file.name.startswith(f"{symbol}_"):
        raise ValueError(f"Filename must begin with {symbol}_. Check --file and --symbol.")
    local_stat = local_file.stat()
    if local_stat.st_size == 0:
        raise ValueError("The local file is empty. Download the trades first.")
    # This is a routing check on the first record, not validation of all records.
    with local_file.open("r", encoding="utf-8-sig") as stream:
        first_record = json.loads(stream.readline(1024 * 1024))
    expected = {
        "source": "alpaca",
        "feed": "sip",
        "ingestion_mode": "historical",
        "symbol": symbol,
    }
    if not isinstance(first_record, dict) or any(
        first_record.get(key) != value for key, value in expected.items()
    ):
        raise ValueError("Expected historical SIP trades from download_historical_trades.py.")
    return local_stat


def properties_or_none(file_client):
    """Only a 404 means absent; permission and connection errors must stop the run."""
    try:
        return file_client.get_file_properties()
    except ResourceNotFoundError:
        return None


def ensure_directory(file_system, path):
    """Create each directory only if it is absent."""
    parts = path.split("/")
    for index in range(1, len(parts) + 1):
        directory = file_system.get_directory_client("/".join(parts[:index]))
        try:
            # IfMissing supplies If-None-Match: *; do not also pass etag="*".
            directory.create_directory(
                match_condition=MatchConditions.IfMissing
            )
        except (ResourceExistsError, ResourceModifiedError):
            # Verify the existing path is accessible; do not hide access failures.
            directory.get_directory_properties()


def require_size(properties, expected_size, path):
    if properties.size != expected_size:
        raise RuntimeError(
            f"Size mismatch at {FILE_SYSTEM}/{path}: Azure has {properties.size:,} "
            f"bytes; local file has {expected_size:,} bytes. No replacement attempted."
        )


def publish_file(source, source_properties, target_path):
    """Rename only if the destination is absent and the source is unchanged."""
    return source.rename_file(
        new_name=f"{FILE_SYSTEM}/{target_path}",
        match_condition=MatchConditions.IfMissing,
        source_etag=source_properties.etag,
        source_match_condition=MatchConditions.IfNotModified,
    )


def run_upload(file_system, local_file, symbol):
    local_stat = validate_local_file(local_file, symbol)
    local_size = local_stat.st_size
    directory_path = f"alpaca/historical/sip/trades/{symbol}"
    target_path = f"{directory_path}/{local_file.name}"
    root_path = local_file.name

    print(f"Storage account: {ACCOUNT_NAME}")
    print(f"Local file: {local_file}")
    print(f"Local size: {local_size:,} bytes ({local_size / (1024 ** 2):.2f} MiB)")
    print(f"Destination: {FILE_SYSTEM}/{target_path}", flush=True)

    # The existing container must be accessible through the Azure CLI identity.
    file_system.get_file_system_properties()
    target = file_system.get_file_client(target_path)
    root = file_system.get_file_client(root_path)
    target_properties = properties_or_none(target)
    root_properties = properties_or_none(root)

    if target_properties is not None and root_properties is not None:
        raise RuntimeError(
            "Both the root file and destination file exist. Both were left unchanged. "
            "Inspect them before deciding which copy to keep."
        )

    if target_properties is not None:
        require_size(target_properties, local_size, target_path)
        print("SKIPPED: The destination already exists.")
        print("Same byte size; contents were not compared.")
        return "SKIPPED"

    if root_properties is not None:
        require_size(root_properties, local_size, root_path)
        ensure_directory(file_system, directory_path)
        moved = publish_file(root, root_properties, target_path)
        print("MOVED: The existing Azure file is now in the project folder.")
        print("No file data was uploaded from your computer.", flush=True)
        require_size(moved.get_file_properties(), local_size, target_path)
        print("Same byte size; contents were not compared.")
        return "MOVED"

    # A future file not yet in Azure is uploaded outside the final input folder.
    ensure_directory(file_system, directory_path)
    ensure_directory(file_system, "_staging")
    staging_path = f"_staging/{uuid4().hex}_{local_file.name}.part"
    staging = file_system.get_file_client(staging_path)
    print(f"Uploading through: {FILE_SYSTEM}/{staging_path}", flush=True)
    try:
        created = staging.create_file(
            match_condition=MatchConditions.IfMissing
        )
        with local_file.open("rb") as stream:
            staging.upload_data(
                data=stream,
                length=local_size,
                overwrite=False,
                etag=created["etag"],
                match_condition=MatchConditions.IfNotModified,
                chunk_size=4 * 1024 * 1024,
                max_concurrency=1,
            )
        current_stat = local_file.stat()
        if (current_stat.st_size, current_stat.st_mtime_ns) != (
            local_size, local_stat.st_mtime_ns
        ):
            raise RuntimeError("Local file changed during upload. Final publication stopped.")
        staging_properties = staging.get_file_properties()
        require_size(staging_properties, local_size, staging_path)
        published = publish_file(staging, staging_properties, target_path)
        print("UPLOADED: The completed file has been moved to the destination.", flush=True)
        require_size(published.get_file_properties(), local_size, target_path)
    except (AzureError, OSError, RuntimeError) as exc:
        raise RuntimeError(
            f"Upload or final verification did not complete: {exc}\n"
            f"Inspect destination: {FILE_SYSTEM}/{target_path}\n"
            f"Inspect staging if present: {FILE_SYSTEM}/{staging_path}\n"
            "No automatic cleanup or overwrite was attempted."
        ) from exc

    print("Uploaded byte count matches the local file. Full-file hashes were not compared.")
    return "UPLOADED"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=DEFAULT_FILE,
                        help="Local historical SIP .jsonl file from the downloader.")
    parser.add_argument("--symbol", default="AAPL", help="Stock symbol; default: AAPL.")
    args = parser.parse_args()

    try:
        credential = AzureCliCredential()
        with DataLakeServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
            credential=credential,
        ) as service:
            run_upload(
                service.get_file_system_client(FILE_SYSTEM),
                args.file.expanduser().resolve(),
                args.symbol.strip().upper(),
            )
    except (AzureError, OSError, ValueError, RuntimeError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        if isinstance(exc, ClientAuthenticationError):
            print("Run az login and select the subscription, then try again.", file=sys.stderr)
        elif isinstance(exc, HttpResponseError) and exc.status_code == 403:
            print(
                "Check the signed-in user's Storage Blob Data Contributor role "
                "on raw (or this account), and the storage account's network rules.",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
