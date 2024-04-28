from pymatris import Downloader

urls = [
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
    "ftp://bob:bob@192.168.1.6:20/tesfile.txt",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
]


def main():
    dm = Downloader()

    for url in urls:
        dm.enqueue_file(url, path="./")

    results = dm.download()
    print(results)


def advanced():
    # Provide custom configuration
    dl = Downloader(
        max_parallel=10,
        max_splits=10,
        max_tries=11,
        overwrite=True,
        all_progress=False,
    )

    for url in urls:
        # Enqueue file to download
        dl.enqueue_file(
            url,
            path="./",
            # You can also provide custom header for HTTP requests
            headers={"User-Agent": "Custom User Agent"},
            # As well as modular override for max_splits, max_tries, overwrite
            max_splits=100,
            max_tries=100,
            overwrite=False,
        )

    result = dl.download()
    print(result)


if __name__ == "__main__":
    main()
