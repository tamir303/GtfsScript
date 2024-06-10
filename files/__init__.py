import requests
import os
from zipfile import ZipFile
from datetime import datetime
import urllib3
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# URL of the GTFS zip file
gtfs_zip_url = "https://gtfs.mot.gov.il/gtfsfiles/israel-public-transportation.zip"

# Directory to save downloaded files
download_dir = "public"

# Create the directory if it doesn't exist
os.makedirs(download_dir, exist_ok=True)


def __download_gtfs_files(url, download_dir) -> None:
    try:
        zip_file_path = os.path.join(download_dir, "israel-public-transportation.zip")

        # Check if the zip file already exists
        if os.path.exists(zip_file_path):
            print("Found GTFS zip file.")

        else:
            print("Downloading GTFS zip file...")
            # Fetch the GTFS zip file
            response = requests.get(url, verify=False, stream=True)
            total_size = int(response.headers.get('content-length', 0))

            if response.status_code == 200:
                # Save the zip file
                with open(zip_file_path, 'wb') as f:
                    # Show progress bar while downloading
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc="Progress") as pbar:
                        for data in response.iter_content(chunk_size=1024):
                            f.write(data)
                            pbar.update(len(data))

                    print("GTFS zip file downloaded successfully.")

            else:
                print("Failed to download GTFS files. Status code:", response.status_code)

        # Extract relevant text files
        __extract_gtfs_text_files(zip_file_path, download_dir)
    except Exception as e:
        print("Error occurred while downloading GTFS files:", e)


def __extract_gtfs_text_files(zip_file_path, download_dir) -> None:
    try:
        print("Extracting GTFS text files...")
        with ZipFile(zip_file_path, 'r') as zip_ref:
            # Extract only the relevant text files
            for file in ["routes.txt", "stop_times.txt", "stops.txt", "trips.txt"]:
                zip_ref.extract(file, download_dir)
        print("GTFS text files extracted successfully.")
    except Exception as e:
        print("Error occurred while extracting GTFS text files:", e)


def __check_gtfs_files_exist(download_dir) -> bool:
    # Check if all GTFS files exist in the directory
    for file in ["routes.txt", "stop_times.txt", "stops.txt", "trips.txt"]:
        if not os.path.exists(os.path.join(download_dir, file)):
            return False
    return True


def get_gtfs_text_files() -> None:
    # Record timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print("Timestamp:", timestamp)

    # Check if GTFS files exist, if not download
    if not __check_gtfs_files_exist(download_dir):
        __download_gtfs_files(gtfs_zip_url, download_dir)
    else:
        print("GTFS files already exist.")
