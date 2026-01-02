import os
import requests
from tqdm import tqdm


def download(url: str) -> str:
    """
    Download ALMA data from the specified URL and save it to a local file.

    Args:
        url (str): URL of the data to download.
    """
    filename = os.path.basename(url)
    try:
        response = requests.get(url, stream=True, verify=True, timeout=(3, 60))
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with (
            open(filename, "wb") as file,
            tqdm(
                desc=filename,
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                leave=False,
            ) as bar,
        ):
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    bar.update(len(chunk))

        # Check if the file was downloaded correctly
        actual_size = os.path.getsize(filename)
        if total_size > 0 and actual_size != total_size:
            raise RuntimeError(
                f"Incomplete download: expected {total_size} bytes, got {actual_size} bytes"
            )
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Download failed: {e}")
    except requests.Timeout as e:
        raise RuntimeError(f"Download timed out: {e}")
    except Exception as e:
        raise RuntimeError(f"Download failed: {e}")

    return filename
