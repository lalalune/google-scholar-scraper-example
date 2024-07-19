from io import BytesIO
import os
from apify_client import ApifyClient
from dotenv import load_dotenv
import requests
import base64
import mimetypes
from PyPDF2 import PdfReader
from urllib.parse import urlparse

load_dotenv()

test_content = "Hello, World!"

api_key = os.getenv("APIFY_API_KEY")

client = ApifyClient(api_key)

# Prepare the Actor input
run_input = {
    "keyword": test_content,
    "maxItems": 50,
    "filter": "all",
    "sortBy": "relevance",
    "articleType": "any",
    "proxyOptions": {"useApifyProxy": True},
    "enableDebugDumps": False,
}

# Run the Actor and wait for it to finish
run = client.actor("kdjLO0hegCjr5Ejqp").call(run_input=run_input)

# Create a directory to store the downloaded files and metadata
output_dir = "downloaded_files"
os.makedirs(output_dir, exist_ok=True)

# Fetch and process Actor results from the run's dataset
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    if "link" in item:
        file_url = item["link"]
        parsed_url = urlparse(file_url)

        if parsed_url.scheme and parsed_url.netloc:
            file_extension = os.path.splitext(file_url)[1]

            file_name = f"{item['cidCode']}{file_extension}"
            file_path = os.path.join(output_dir, file_name)

            # Check if the file has already been downloaded
            if os.path.exists(file_path):
                print(f"File already exists: {file_name}")
                continue

            response = requests.get(file_url)

            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "").lower()

                if "pdf" in content_type:
                    file_extension = ".pdf"
                elif "html" in content_type:
                    file_extension = ".html"
                else:
                    file_extension = mimetypes.guess_extension(content_type) or ".txt"

                file_name = f"{item['cidCode']}{file_extension}"
                file_path = os.path.join(output_dir, file_name)

                with open(file_path, "wb") as file:
                    file.write(response.content)
                    print(f"Downloaded: {file_name}")

                # Check if the downloaded file is a valid PDF
                if file_extension == ".pdf":
                    try:
                        with open(file_path, "rb") as file:
                            pdf = PdfReader(file)
                            if len(pdf.pages) == 0:
                                print(f"Error: Empty PDF - {file_name}")
                    except:
                        print(f"Error: Invalid PDF - {file_name}")

            else:
                print(f"Failed to download: {file_url}")
                # Extract and save the summary and other available information
                summary = {
                    "title": item.get("title", ""),
                    "authors": item.get("authors", ""),
                    "year": item.get("year", ""),
                    "abstract": item.get("searchMatch", ""),
                }
                summary_file_name = f"{item['cidCode']}_summary.txt"
                summary_file_path = os.path.join(output_dir, summary_file_name)

                with open(summary_file_path, "w", encoding="utf-8") as file:
                    file.write(str(summary))
                    print(f"Saved summary: {summary_file_name}")
        else:
            print(f"Invalid URL: {file_url}")