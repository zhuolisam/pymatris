# Pymatris 📂


[![Pyversions](https://img.shields.io/pypi/pyversions/pymatris.svg?style=flat-square)](https://pypi.python.org/pypi/pymatris)

Parallel file downloader for HTTP/HTTPS, FTP and SFTP protocols, built using Python.


🔗[Demo](https://youtu.be/v8pWW8eO5jQ)
🔗[Demo cont'd](https://youtu.be/v7ME6Z11KII)
🔗[Demo cont'd](https://youtu.be/1iFGObABnO0)

### Installation

```
pip install pymatris
```

##  Usage

Initialize Downloader
```python
from pymatris import Downloader

dl = Downloader()
```
Enqueue file to download
```python

dl.enqueue_file("https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet", path="./")

```

Start downloading files and view results
```python

results = dl.download()
print(results)

```

### Example Usage

from [main.py](https://github.com/zhuolisam/pymatris/blob/main/main.py)

```python
from pymatris import Downloader
urls = [
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-02.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-03.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-04.parquet",
    "https://storage.data.gov.my/pricecatcher/pricecatcher_2022-05.parquet",
    "sftp://belle:belle@192.168.1.6/home/belle/files/testfile.txt",
    "ftp://bob:bob@192.168.1.6:20/nonexistent.txt",
]

dm = Downloader()

for url in urls:
    dm.enqueue_file(url, path="./")

results = dm.download()


print(results)
>> Success:
>> pricecatcher_2022-01.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet
>> pricecatcher_2022-02.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-02.parquet
>> pricecatcher_2022-03.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-03.parquet
>> pricecatcher_2022-04.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-04.parquet
>> pricecatcher_2022-05.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-05.parquet
>> testfile.txt sftp://belle:belle@192.168.1.6/home/belle/files/testfile.txt

>> Errors:
>> (ftp://bob:bob@192.168.1.6:20/tesfile.txt,
>> ConnectionRefusedError(61, "Connect call failed ('192.168.1.6', 20)"))

```

### Advanced Usage
Visit [main.py](https://github.com/zhuolisam/pymatris/blob/main/main.py) for advanced usage.


## Program Architecture
![Architecture.png](static/architecture.png)

Under the hood, `pymatris.Downloader` uses a global queue to manage the download tasks. `pymatris.Downloader.enqueue_file()` will add url to download queue, and `pymatris.Downloder.download()` will download the files in parallel. 


`pymatris.Downloader` depends on [`ProtocolResolver`]((https://github.com/zhuolisam/pymatris/blob/main/pymatris/protocol_handler/__init__.py)) to resolve the url scheme and route to the correct protocol handler. 

Pymatris uses asyncio-based clients (`aiohttp`, `aioftp`, `asyncssh`) to download files in parallel, with asychronous I/O operations using `aiofiles`.


#### Results and Error Handling
`pymatris.Downloader.download()` returns a `Results` object, which is a list of the filenames that have been downloaded. `Results` object has two attributes, `success` and `errors`. 

`success` is a list of named tuples, where each named tuple contains `.path` the filepath and `.url` the url. 

`errors` is a list of named tuples, where each named tuple contains `.filepath_partial` the intended filepath, `.url` the url, `.exception` an Exception or aiohttp.ClientResponse that occurred during download.




## CLI
Pymatris also provides a command line interface to download files in parallel.
In your terminal, run the following command to download files in parallel.
```bash
# Insert single url as argument
pymatris https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet 

# Or multiple urls 
pymatris https://storage.data.gov.my/pricecatcher/pricecatcher_2022-01.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-02.parquet https://storage.data.gov.my/pricecatcher/pricecatcher_2022-03.parquet
```

```bash
usage: pymatris [-h] [--max-parallel MAX_PARALLEL] [--max-splits MAX_SPLITS] 
                [--max-tries MAX_TRIES] [--timeouts TIMEOUTS] [--dir DIR] 
                [--overwrite] [--quiet] [--show-errors] [--verbose] URLS [URLS ...]

pymatris: Parallel download manager for HTTP/HTTPS/FTP/SFTP protocols.

positional arguments:
  URLS                  URLs of files to be downloaded.

options:
  -h, --help            show this help message and exit
  --max-parallel MAX_PARALLEL
                        Maximum number of parallel file downloads.
  --max-splits MAX_SPLITS
                        Maximum number of parallel connections per file (only if protocol and server is supported).
  --max-tries MAX_TRIES
                        Maximum number of download attempt per url.
  --timeouts TIMEOUTS   Maximum timeouts per url.
  --dir DIR             Directory to which downloaded files are saved.
  --overwrite           Overwrite if file exists. Only one url with the clashing name will overwrite the file.
  --quiet               Show progress indicators and file retries if any during download.
  --show-errors         Show failed downloads with its errors to stderr.
  --verbose             Log debugging output while transferring the files.

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
* python 3.9 or above
* aiohttp
* aioftp
* asyncssh
* aiofiles
* tqdm


### TODO
- [ ] Add support for public key authentication for FTP and SFTP protocol handler.
- [ ] Resolve `PytestUnraisableExceptionWarning` due to pytestservers running on separate threads. [ref](https://github.com/pytest-dev/pytest/issues/9825)
- [ ] Add better concurrency support for FTP protocol, as `aioftp` by default allow only one data connection per client session.
- [ ] Better error handling and logging.


### Acknowledgements 
* [aiofiles](https://github.com/Tinche/aiofiles)
* [pytest-localserver](https://github.com/pytest-dev/pytest-localserver)
* [asyncssh](https://github.com/ronf/asyncssh)
* [Pyaiodl](https://github.com/aryanvikash/Pyaiodl)
* [aiodl](https://github.com/cshuaimin/aiodl)
* [parfive](https://github.com/Cadair/parfive)