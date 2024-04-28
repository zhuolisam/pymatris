# Pymatris 📂


[![Pyversions](https://img.shields.io/pypi/pyversions/pymatris.svg?style=flat-square)](https://pypi.python.org/pypi/pymatris)

Parallel file downloader for HTTP/HTTPS, FTP and SFTP protocols, built using Python.


## Installation

```
pip install pymatris
```

#  Usage

* Initialize Downloader
```python
from pymatris import Downloader

dl = Downloader()
```
* Enqueue file to download
```python

dl.enqueue_file("https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet", path="./")

```

* Start downloading files
```python

dl.download()

```

* View results
```python
result = dl.download()
```



Under the hood, `pymatris.Downloader` uses a global queue to manage the download tasks. `pymatris.Downloader.enqueue_file()` will add url to download queue, and `pymatris.Downloder.download()` will download the files in parallel. Pymatris uses asyncio to download files in parallel, with asychronous I/O operations using aiofiles, hence enabling faster downloads.


#### Results and Error Handling
`pymatris.Downloader.download()` returns a `Results` object, which is a list of the filenames that have been downloaded. `Results` object has two attributes, `success` and `errors`. 

`success` is a list of named tuples, where each named tuple contains `.path` the filepath and `.url` the url. 

`errors` is a list of named tuples, where each named tuple contains `.filepath_partial` the intended filepath, `.url` the url, `.exception` an Exception or aiohttp.ClientResponse that occurred during download.


### Example Usage

from [main.py](https://github.com/zhuolisam/pymatris/blob/main/main.py)

```python
from pymatris import Downloader
urls = [
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
    "ftp://bob:bob@192.168.1.6:20/tesfile.txt",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
]



dm = Downloader()

for url in urls:
    dm.enqueue_file(url, path="./")

results = dm.download()


print(results)
>> Success:
>> pricecatcher_2022-01.csv https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.csv
>> pricecatcher_2022-01.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet

>> Errors:
>> (ftp://bob:bob@192.168.1.6:20/tesfile.txt,
>> ConnectionRefusedError(61, "Connect call failed ('192.168.1.6', 20)"))

```


```
dl = Downloader(
    max_parallel=5,
    max_splits=5,
    overwrite=False,
    dir="./",
    all_progress=True
)
```


### Advanced Usage
Visit [main.py](https://github.com/zhuolisam/pymatris/blob/main/main.py) for advanced usage.


### CLI
Pymatris also provides a command line interface to download files in parallel.
In your terminal, run the following command to download files in parallel.
```bash
# Insert single url as argument
pymatris https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet 

# Or multiple urls 
pymatris https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-02.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-03.parquet
```

```bash
  $ pymatris --help
  usage: pymatris [-h] [--max-parallel MAX_PARALLEL] [--max-splits MAX_SPLITS]
                [--overwrite] [--quiet] [--dir DIR] [--show-errors SHOW_ERRORS]
                [--timeouts TIMEOUTS] [--max-tries MAX_TRIES]
                URLS [URLS ...]

```


#### Arguments

**To provide path to save files, use --dir option. By default files will be saved in current directory.**

```bash
pymatris --dir "./" <urls>
```

**To overwrite existing files, use --overwrite option. By default, files will not be overwritten.**

```bash
pymatris --overwrite <urls>
```
_Assuming your have "pricecatcher_2022-01.parquet" file in your current directory, running above command will overwrite the existing file.
During download, Pymatris creates tempfile to download files, if download is interrupted, rest assured that your existing files are safe, and tempfiles will be deleted._

**To configure number of parallel downloads, use --max-parallel option. By default, 5 parallel downloads are allowed.**

```bash
pymatris --max-parallel 10 <urls>
```
_Pymatris uses asyncio to download files in parallel. By default, 5 files are downloaded in parallel. You can increase or decrease the number of parallel downloads._



**To configure number of parallel download parts per file, use --max-splits option. By default, 5 parts are downloaded in parallel for each file.**

```bash
pymatris --max-splits 10 <urls>
```
_This is only available for HTTP/HTTPS and SFTP protocols. Currently, FTP protocol does not support multipart downloads._

**To configure number of retries for failed downloads, use --max-tries option. By default, 5 retries are allowed.**

```bash

pymatris --max-tries 10 <urls>
```

**To hide progress bar, use --quiet option. By default, progress bar is shown.**

```bash
pymatris --quiet <urls>
```



### Requirements
* Python 3.9 or above
* aiohttp
* aioftp
* asyncssh
* aiofiles
* tqdm


### TODO
- [ ] Add better concurrency support for FTP protocol.
- [ ] Better error handling and logging for FTP and SFTP protocols.


### Acknowledgements 
* [aiofiles](https://github.com/Tinche/aiofiles)
* [pytest-localserver](https://github.com/pytest-dev/pytest-localserver)
* [asyncssh](https://github.com/ronf/asyncssh)
* [Pyaiodl](https://github.com/aryanvikash/Pyaiodl)
* [aiodl](https://github.com/cshuaimin/aiodl)
* [parfive](https://github.com/Cadair/parfive)