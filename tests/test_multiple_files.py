from pymatris import Downloader


def test_name_clash(singlepartserver, sftp_server, ftp_server, tmp_path):
    pathlists = list(ftp_server.get_file_contents("testfile.txt", style="url"))

    ftpfile = pathlists[0]

    dm = Downloader()
    dm.enqueue_file(singlepartserver.url, path=tmp_path)
    dm.enqueue_file(f"{sftp_server.url}/testfile.txt", path=tmp_path)
    dm.enqueue_file(ftpfile["path"], path=tmp_path)
    res = dm.download()

    files = [*tmp_path.iterdir()]
    print(files)
    assert len(res.success) == 3
    assert len(res.success) == len(files)
    assert set([*res]) == set([str(file) for file in files])

    assert set([file.name for file in files]) == set(
        ["testfile.txt", "testfile.1.txt", "testfile.2.txt"]
    )


def test_overwrite(singlepartserver, sftp_server, ftp_server, tmp_path):
    pathlists = list(ftp_server.get_file_contents("testfile.txt", style="url"))

    ftpfile = pathlists[0]

    dm = Downloader()
    dm.enqueue_file(singlepartserver.url, path=tmp_path)
    dm.enqueue_file(f"{sftp_server.url}/testfile.txt", path=tmp_path)
    dm.enqueue_file(ftpfile["path"], path=tmp_path)
    res = dm.download()

    files = [*tmp_path.iterdir()]

    assert len(res.success) == 3
    assert len(res.success) == len(files)
    assert set([*res]) == set([str(file) for file in files])

    assert set([file.name for file in files]) == set(
        ["testfile.txt", "testfile.1.txt", "testfile.2.txt"]
    )

    # Queue and download same files, only one will overwrite existing testfile.txt
    dm.enqueue_file(singlepartserver.url, path=tmp_path)
    dm.enqueue_file(f"{sftp_server.url}/testfile.txt", path=tmp_path)
    dm.enqueue_file(ftpfile["path"], path=tmp_path)
    res2 = dm.download()

    files2 = [*tmp_path.iterdir()]

    assert len(res2.success) == 3
    assert len(files2) == 5

    assert set([file.name for file in files2]) == set(
        [
            "testfile.txt",
            "testfile.1.txt",
            "testfile.2.txt",
            "testfile.3.txt",
            "testfile.4.txt",
        ]
    )
