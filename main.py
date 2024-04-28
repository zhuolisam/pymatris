from pymatris import Downloader


def main():
    dm = Downloader()
    dm.enqueue_file("https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.csv")
    dm.enqueue_file("ftp://bob:bob@192.168.1.6:21/tesfile.txt")
    dm.enqueue_file(
        "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet"
    )
    # dm.enqueue_file("https://yooo.com/file.parquet", max_tries=1)
    # dm.enqueue_file("ftp://ftp.swpc.noaa.gov/pub/warehouse/2011/2013_SRS.tar.gz")
    results = dm.download()
    print(results)


if __name__ == "__main__":
    main()
