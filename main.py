'''

Python program to get the size of folders in HDFS and send them to Prometheus Pushgateway.
For testing, you can use the URL addresses that are in the comments.
For use in a production environment, it is better to use environment variables that are passed to the script.

# Author: Josef Štrba

Instructions for running:

For local testing, you need to have Prometheus and Pushgateway servers running.

Env variables are entered from the terminal (example for a production environment):
$env:HDFS_PROXY_URL="http://srchprod-ko-proxy.organic.ftxt.iszn.cz:8070"
$env:PUSHGATEWAY_URL="http://localhost:9091"
$env:FULLTEXT_URL="/fulltext/volume/1/complete"
$env:SERVICE="Fulltext-production"

Then run the script via the terminal, the variables will be loaded automatically and
the program will be executed with these URLs.

'''

from prometheus_client import Gauge, CollectorRegistry, push_to_gateway
from hdfs import InsecureClient
import logging
import os

# URL variables for this script for production testing:
# Proxy_url = "http://srchprod-ko-proxy.organic.ftxt.iszn.cz:8070"
# PushGW_url = "http://localhost:9091"
# Fulltext_url = "/fulltext/volume/1/complete"
# Service = "fulltext-production"

# URL variables for this script for debug testing:
# Proxy_url = "http://srchdev-proxy.dev.dszn.cz"
# PushGW_url = "http://localhost:9091"
# Fulltext_url = "/fulltext-rum/volume/1/complete"
# Service = "fulltext-debug"

# Passing URLs using environment variables
Proxy_url = os.environ.get("HDFS_PROXY_URL")
PushGW_url = os.environ.get("PUSHGATEWAY_URL")
Fulltext_url = os.environ.get("FULLTEXT_URL")
Service = os.environ.get("SERVICE")


# Logging settings
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("hdfs").setLevel(logging.CRITICAL + 1)

# Initialize HDFS client
HDFS_URL = Proxy_url
client = InsecureClient(HDFS_URL)

# Pushgateway server address
JOB_NAME = "disk_usage"

# Recursive calculation of folder sizes
def list_dirs_recursive(path, registry, file_size_metric, base_path):
    total_size = 0

    try:
        statuses = client.list(path, status=True)
    except Exception as e:
        logging.warning(f"There was an error while reading the folder {path}: {e}")
        return 0

    for name, item in statuses:
        full_path = f"{path}/{name}"
        if item["type"] == "DIRECTORY":
            size = list_dirs_recursive(full_path, registry, file_size_metric, base_path)
            total_size += size
        else:
            size = item["length"] # / (1024 * 1024) # To convert to MB
            total_size += size # / (1024 * 1024) # To convert to MB

    rel_path = os.path.relpath(path, base_path).replace("\\", "/")
    if rel_path == ".":
        rel_path = "/"
    else:
        rel_path = f"/{rel_path}"

    file_size_metric.labels(folder=rel_path).set(total_size)

    logging.info(f"   Folder: {rel_path}")
    logging.info(f"       - Size: {total_size} Bytes")

    return total_size

# Check for existence of merge.done (only folders with merge.done are processed)
def has_merge_done(path):
    control_path = f"{path}/control"
    try:
        statuses = client.list(control_path, status=True)
    except Exception:
        return False
    for name, item in statuses:
        if name == "merge.done":
            return True
    return False

# Processing only the latest folder
def process_latest_valid_folder(root_path):
    try:
        statuses = client.list(root_path, status=True)
    except Exception as e:
        logging.error(f"The path could not be reached {root_path}: {e}")
        return

    # Processing metrics using Prometheus
    registry = CollectorRegistry()
    file_size_metric = Gauge("disk_usage_bytes",
                             "Total size of files in this folder: ", ["folder"], registry=registry)

    valid_folders = []
    for name, item in statuses:
        if item["type"] == "DIRECTORY":
            folder_path = f"{root_path}/{name}"
            if has_merge_done(folder_path):
                valid_folders.append(name)

    if not valid_folders:
        logging.info("❗ No folders with merge.done found.")
        return

    latest_folder = sorted(valid_folders)[-1]
    folder_path = f"{root_path}/{latest_folder}/barrels/current"

    logging.info(f"\n📁 Only latest folder is being browsed: {folder_path}:\n")
    list_dirs_recursive(folder_path, registry, file_size_metric, folder_path)

    try:
        push_to_gateway(PushGW_url, job=JOB_NAME, registry=registry,
                        grouping_key={"instance": Service})
        logging.info("\n✅ Metrics were successfully pushed to the Pushgateway.")
    except Exception as e:
        logging.error(f"\n❌ An error occured while pushing the metrics: {e}")


if __name__ == "__main__":
        process_latest_valid_folder(Fulltext_url)
