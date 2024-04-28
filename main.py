from pymatris import Downloader, SessionConfig

urls = [
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-02.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-03.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-04.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-05.parquet",
    "sftp://belle:belle@192.168.1.6/home/belle/files/testfile.txt",
    "ftp://bob:bob@192.168.1.6:21/nonexistent.txt",
]


def main():
    dm = Downloader()

    for url in urls:
        dm.enqueue_file(url, path="./output")

    result = dm.download()
    print(result)


def advanced():
    # Provide custom configuration
    dl = Downloader(
        max_parallel=10,  # Custom parallel tasks
        max_splits=10,  # Custom file spltting (only for HTTP requests)
        max_tries=10,  # Custom retry
        overwrite=True,  # Make overwrite to True
        all_progress=False,  # Disable main progress bar
        session_config=SessionConfig(
            timeouts=30,  # Set timeouts to 30 seconds
            log_level="DEBUG",  # Debug
            file_progress=False,  # Disable file progress bar
            headers={
                "User-Agent": "Custom User Agent"
            },  # Setup custom agent headers (only for HTTP requests)
        ),
    )

    for url in urls:
        # Enqueue file to download
        dl.enqueue_file(
            url,
            path="./output",
            # You can also provide custom header for HTTP requests for particular file
            headers={"User-Agent": "Custom User Agent"},
            # As well as particular override for max_splits, max_tries, overwrite
            max_splits=20,
            max_tries=20,
            overwrite=True,
        )

    result = dl.download()
    print(result)


if __name__ == "__main__":
    advanced()
