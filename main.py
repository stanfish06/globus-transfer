import json
import os
from datetime import datetime

import globus_sdk
from globus_sdk import (
    AccessTokenAuthorizer,
    NativeAppAuthClient,
    TransferClient,
    TransferData,
)

CLIENT_ID = "a3b310f0-65dd-411e-9058-e845e2f0be12"
TOKEN_FILE = "globus_tokens.json"

source_id = "7f0e40f3-e8c3-11ef-a8c4-0affe48c30b5"
source_path = "/R/Heemskerk_Lab/Seth-06/BMPIntegralData"
target_id = "454f457e-a41b-4807-8775-d132f15a228f"
target_path = "/scratch/iheemske_root/iheemske99/shared_data/Seth-BMP"

max_files = 5
# transfer 3TB at a time
batch_limit = 0.5 * 1024 * 1024 * 1024 * 1024
generate_plan = False


def get_transfer_client():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)
            transfer_token = tokens.get("transfer_token")
            if transfer_token:
                print("using cached tokens")
                return TransferClient(authorizer=AccessTokenAuthorizer(transfer_token))

    print("need to re-authenticate...")
    client = NativeAppAuthClient(CLIENT_ID)
    client.oauth2_start_flow(
        requested_scopes="urn:globus:auth:scope:transfer.api.globus.org:all"
    )
    authorize_url = client.oauth2_get_authorize_url()
    print(f"{authorize_url}\n")
    auth_code = input("code: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_token = token_response.by_resource_server["transfer.api.globus.org"][
        "access_token"
    ]
    with open(TOKEN_FILE, "w") as f:
        json.dump({"transfer_token": transfer_token}, f)
    print(f"Tokens saved to {TOKEN_FILE}")
    return TransferClient(authorizer=AccessTokenAuthorizer(transfer_token))


def login_with_required_scopes(scopes):
    client = NativeAppAuthClient(CLIENT_ID)
    client.oauth2_start_flow(requested_scopes=scopes)
    authorize_url = client.oauth2_get_authorize_url()
    print(f"\n{'=' * 70}")
    print("ADDITIONAL CONSENT REQUIRED")
    print(f"{'=' * 70}")
    print(f"\n{authorize_url}\n")
    auth_code = input("Enter code: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_token = token_response.by_resource_server["transfer.api.globus.org"][
        "access_token"
    ]

    with open(TOKEN_FILE, "w") as f:
        json.dump({"transfer_token": transfer_token}, f)

    return TransferClient(authorizer=AccessTokenAuthorizer(transfer_token))


def fetch_folder_content(transfer_client, collection_id, path):
    for item in transfer_client.operation_ls(collection_id, path=path):
        yield item


def format_file_size(bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"


def calculate_folder_size(transfer_client, collection_id, path):
    total_size = 0
    num_files = 0
    for f in fetch_folder_content(transfer_client, collection_id, path):
        if f["type"] == "file":
            total_size += f.get("size", 0)
            num_files += 1
        elif f["type"] == "dir":
            total_size_dir, num_files_dir = calculate_folder_size(
                transfer_client, collection_id, f"{path.rstrip('/')}/{f['name']}"
            )
            total_size += total_size_dir
            num_files += num_files_dir
        else:
            raise ValueError(f"unknown type {f['type']}")
    return total_size, num_files


def check_folder(transfer_client, collection_id, path):
    items = {}
    n_checked = 0
    for f in fetch_folder_content(transfer_client, collection_id, path):
        n_checked += 1
        if f["type"] == "file":
            items[f["name"]] = {
                "type": f["type"],
                "path": f"{path.rstrip('/')}/{f['name']}",
                "size": f["size"],
                "size_h": format_file_size(f["size"]),
            }
        elif f["type"] == "dir":
            total_size, num_files = calculate_folder_size(
                transfer_client, collection_id, f"{path.rstrip('/')}/{f['name']}"
            )
            items[f["name"]] = {
                "type": f["type"],
                "path": f"{path.rstrip('/')}/{f['name']}",
                "num_files": num_files,
                "size": total_size,
                "size_h": format_file_size(total_size),
            }
        else:
            raise ValueError(f"unknown type {f['type']}")
        print(f"[INFO] {f['name']} -> {items[f['name']]}")
        if n_checked == max_files:
            break
    return items


def create_transfer_batch(files: dict, batch_size_limit):
    batches = []
    current_batch = []
    current_batch_path = []
    current_batch_size = 0
    current_total_num_files = 0

    for name, info in files.items():
        file_size = info["size"]

        # start a new batch when exceeds limit
        if current_batch and (current_batch_size + file_size) > batch_size_limit:
            batches.append(
                {
                    "batch_id": len(batches) + 1,
                    "file_names": current_batch,
                    "file_paths": current_batch_path,
                    "total_size": current_batch_size,
                    "total_size_h": format_file_size(current_batch_size),
                    "num_items": len(current_batch),
                    "total_num_files": current_total_num_files,
                    "status": "pending",
                }
            )
            current_batch = []
            current_batch_path = []
            current_batch_size = 0
            current_total_num_files = 0

        if info["type"] == "file":
            current_total_num_files += 1
        elif info["type"] == "dir":
            current_total_num_files += info["num_files"]
        current_batch.append(name)
        current_batch_path.append(info["path"])
        current_batch_size += file_size

    # Add the last batch
    if current_batch:
        batches.append(
            {
                "batch_id": len(batches) + 1,
                "file_names": current_batch,
                "file_paths": current_batch_path,
                "total_size": current_batch_size,
                "total_size_h": format_file_size(current_batch_size),
                "num_items": len(current_batch),
                "total_num_files": current_total_num_files,
                "status": "pending",
            }
        )

    return batches


def save_batches(batches, filename="transfer_batches.json"):
    data = {
        "source_id": source_id,
        "source_path": source_path,
        "target_id": target_id,
        "target_path": target_path,
        "batch_size_limit": batch_limit,
        "batch_size_limit_h": format_file_size(batch_limit),
        "total_batches": len(batches),
        "batches": batches,
    }

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)


def load_batches(filename="transfer_batches.json"):
    if not os.path.exists(filename):
        raise FileNotFoundError(f"{filename} not found")
    with open(filename, "r") as f:
        return json.load(f)


def get_batch(data, batch_id):
    for i, batch in enumerate(data["batches"]):
        if batch["batch_id"] == batch_id:
            return i, batch
    return None, None


def change_batch_status(data, batch_id, new_status):
    i, batch = get_batch(data, batch_id)

    if not batch:
        print(f"[ERROR] batch {batch_id} not found")
        return False

    batch["status"] = new_status
    batch["status_updated_at"] = datetime.now().isoformat()
    print(batch)

    save_batches(data["batches"])
    return True


def submit_transfer(transfer_client, batch, batch_id):
    tdata = TransferData(
        source_endpoint=source_id,
        destination_endpoint=target_id,
        label=f"transfer_batch_{batch_id}",
        sync_level="checksum",
        verify_checksum=True,
        preserve_timestamp=True,
        fail_on_quota_errors=True,
    )
    for file_path in batch["file_paths"]:
        rel_path = file_path.replace(source_path, "").lstrip("/")
        target_file = f"{target_path.rstrip('/')}/{rel_path}"
        tdata.add_item(file_path, target_file, recursive=True)
        print(f"  + {file_path} -> {target_file}")
    try:
        print(f"[SUBMIT] Transfer task for batch {batch_id}...")
        task = transfer_client.submit_transfer(tdata)
    except globus_sdk.TransferAPIError as err:
        if err.info.consent_required:
            print("\n[ERROR] Consent required for collections!")
            print("Required scopes:", err.info.consent_required.required_scopes)
            print("\nYou need to re-authenticate with collection access...")
            print("Delete globus_tokens.json and run again, OR:")
            print("Run this to grant consent:")

            # Re-login with required scopes
            new_client = login_with_required_scopes(
                err.info.consent_required.required_scopes
            )
            # Retry the transfer
            task = new_client.submit_transfer(tdata)
        else:
            raise


# either transfer, none, or re-transfer
actions = {
    "1": "transfer",
    "2": "none",
    "3": "none",
    "4": "none",
}
dry_run = False


def main():
    transfer_client = get_transfer_client()
    if generate_plan:
        files = check_folder(
            transfer_client=transfer_client, collection_id=source_id, path=source_path
        )
        batches = create_transfer_batch(files, batch_size_limit=batch_limit)
        save_batches(batches)
    else:
        batches = load_batches()
        for batch_id, action in actions.items():
            print(f"[ACTION] {action} -> {batch_id}")
            if action == "none":
                print(f"skip {batch_id}")
            elif action == "transfer":
                _, batch = get_batch(batches, int(batch_id))
                print(f"batch {batch_id} has status {batch['status']}")
                if batch["status"] == "pending":
                    print(f"will transfer {batch['file_names']} to {target_path}")
                    if not dry_run:
                        submit_transfer(transfer_client, batch, batch_id)
                    change_batch_status(batches, int(batch_id), "transferred")
                elif batch["status"] == "transferred":
                    print(f"batch {batch_id} was transferred already, so do nothing.")
                elif batch["status"] == "completed":
                    print(f"batch {batch_id} was completed, so do nothing.")
            elif action == "re-transfer":
                _, batch = get_batch(batches, int(batch_id))
                print(f"batch {batch_id} has status {batch['status']}")
                print(f"will transfer {batch['file_names']} to {target_path}")
                if not dry_run:
                    if batch["status"] == "completed":
                        print(f"batch {batch_id} was completed, so do nothing.")
                    else:
                        submit_transfer(transfer_client, batch, batch_id)
            print("")


if __name__ == "__main__":
    main()
