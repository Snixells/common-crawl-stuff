import gzip
import shutil
import time
from io import BytesIO

import requests

BASE_URL = "https://data.commoncrawl.org"
CRAWL_ID = "CC-MAIN-2023-40"
CRAWL_URL = f"{BASE_URL}/crawl-data/{CRAWL_ID}"


def make_request_and_download(request_url, local_file_path, max_retries=5):
    retries = 0
    backoff = 1  # Start with 1 second

    while retries < max_retries:
        response = requests.get(request_url)
        if response.status_code == 200:
            print(f"{request_url} downloaded successfully")

            # Decompress directly from response content
            with gzip.open(BytesIO(response.content), 'rb') as f_in:
                with open(local_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"{request_url} saved successfully")
            return response
        elif response.status_code == 503:  # 503 Service Unavailable or SlowDown equivalent
            print(f"Trying request again. Retries: {retries}, backoff: {backoff}")
            time.sleep(backoff)
            retries += 1
            backoff *= 2  # Exponentially increase the backoff
        else:
            response.raise_for_status()

    raise Exception("Max retries exceeded")


# make_request_and_download(f"{CRAWL_URL}/cc-index.paths.gz", "data/cc-index.paths")
#
make_request_and_download(f"{BASE_URL}/cc-index/collections/CC-MAIN-2023-40/indexes/cluster.idx", "data/cluster.idx")
