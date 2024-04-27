from pymatris import Downloader
from pathlib import Path

THIS_DIR = Path(__file__).parent

test_file_dir = THIS_DIR / "static/ftp_testfile.txt"


# def test_name_clash(singlepartserver, sftp_server, ftpserver, tmp_path):
#     ftp_url = ftpserver.put_files(
#         {"src": str(test_file_dir), "dest": "testfile.txt"},
#         style="url",
#         anon=False,
#         overwrite=True,
#     )

#     dm = Downloader()
#     dm.enqueue_file(singlepartserver.url, path=tmp_path)
#     dm.enqueue_file(f"{sftp_server.url}/test_folder/testfile.txt", path=tmp_path)
#     dm.enqueue_file(ftp_url[0], path=tmp_path)
#     res = dm.download()

#     files = [*tmp_path.iterdir()]
#     print(files)
#     assert len(res.success) == 3
#     assert len(res.success) == len(files)
#     assert set([*res]) == set([str(file) for file in files])

#     assert set([file.name for file in files]) == set(
#         ["testfile.txt", "testfile.1.txt", "testfile.2.txt"]
#     )


# def test_overwrite(singlepartserver, sftp_server, ftpserver, tmp_path):
#     ftp_url = ftpserver.put_files(
#         {"src": str(test_file_dir), "dest": "testfile.txt"},
#         style="url",
#         anon=False,
#         overwrite=True,
#     )

#     dm = Downloader()
#     dm.enqueue_file(singlepartserver.url, path=tmp_path)
#     dm.enqueue_file(f"{sftp_server.url}/test_folder/testfile.txt", path=tmp_path)
#     dm.enqueue_file(ftp_url[0], path=tmp_path)
#     res = dm.download()

#     files = [*tmp_path.iterdir()]

#     assert len(res.success) == 3
#     assert len(res.success) == len(files)
#     assert set([*res]) == set([str(file) for file in files])

#     assert set([file.name for file in files]) == set(
#         ["testfile.txt", "testfile.1.txt", "testfile.2.txt"]
#     )

#     # Queue and download same files, only one will overwrite existing testfile.txt
#     dm.enqueue_file(singlepartserver.url, path=tmp_path)
#     dm.enqueue_file(f"{sftp_server.url}/test_folder/testfile.txt", path=tmp_path)
#     dm.enqueue_file(ftp_url[0], path=tmp_path)
#     res2 = dm.download()

#     files2 = [*tmp_path.iterdir()]

#     assert len(res2.success) == 3
#     assert len(files2) == 5

#     assert set([file.name for file in files2]) == set(
#         [
#             "testfile.txt",
#             "testfile.1.txt",
#             "testfile.2.txt",
#             "testfile.3.txt",
#             "testfile.4.txt",
#         ]
#     )
