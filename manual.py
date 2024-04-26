import asyncio
import aiohttp
from urllib.parse import urlsplit
from pathlib import Path
from tqdm import tqdm


async def download_file(url):
    spliturl = urlsplit(url)
    url_file_name = spliturl.path.split("/")[-1]
    cur_path = Path.cwd()
    output_filepath = Path(cur_path) / url_file_name
    # output_filepath.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            total_size = int(response.headers.get("Content-Length", 0))
            downloaded = 0
            print(f"Downloading {url_file_name}")
            bar = tqdm(
                initial=downloaded,
                dynamic_ncols=True,
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            )

            if response.status == 200:
                with open(f"{output_filepath}", "wb") as f:
                    while True:
                        chunk = await response.content.read(1024**1000000)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        # print(
                        #     f"Downloaded {downloaded/total_size} bytes for file {url_file_name}"
                        # )
                        bar.update(len(chunk))
            else:
                print("Failed")


def build_blocks(content_size, max_splits):
    """
    Args:
        content_size (int): total file size in bytes
        max_splits (int): how many parts to split the file into, one part will be equal to content_size // max_splits
    """
    pass


async def download_block(blocks, writer, bar):
    pass


urls = [
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.csv",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
]


async def main():
    await asyncio.gather(*[download_file(url) for url in urls])


asyncio.run(main())
