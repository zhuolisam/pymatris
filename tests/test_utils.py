from pymatris.utils import (
    allocate_tempfile,
    replace_tempfile,
    remove_file,
    replace_file,
    get_filepath,
    replacement_filename,
)

from .conftest import validate_test_file_content


def test_allocate_tempfile(tmp_path):
    original_path = tmp_path / "test.txt"
    open(original_path, "a").close()  # create original file
    tempfile_path = allocate_tempfile(original_path)
    assert tempfile_path.exists()
    assert tempfile_path.name.startswith("test")
    assert tempfile_path.suffix == ".matris"


def test_replace_tempfile(tmp_path):
    original_path = tmp_path / "test.txt"
    open(original_path, "a").close()  # create original file

    tempfile_path = allocate_tempfile(original_path)
    assert tempfile_path.exists()

    with open(tempfile_path, "w") as writer:
        writer.write("Tempfile Content")

    new_original_path = replace_tempfile(tempfile_path)
    validate_test_file_content(new_original_path, "Tempfile Content")
    assert new_original_path.exists()
    assert not tempfile_path.exists()
    assert new_original_path == original_path


def test_remove_file(tmp_path):
    file_path = tmp_path / "test.txt"
    open(file_path, "a").close()  # create file
    remove_file(file_path)
    assert not file_path.exists()


def test_replace_file(tmp_path):
    old_file_path = tmp_path / "old.txt"
    new_file_path = tmp_path / "new.txt"
    open(old_file_path, "a").close()  # create old file
    open(new_file_path, "a").close()  # create new file

    replaced_path = replace_file(old_file_path, new_file_path)
    assert replaced_path.exists()
    assert not old_file_path.exists()
    assert replaced_path == new_file_path


def test_get_filepath_existing_file(tmp_path):
    filepath = tmp_path / "test.txt"
    open(filepath, "a").close()  # create existing file

    finalpath = get_filepath(filepath, overwrite=False)
    assert finalpath != filepath
    assert str(finalpath) == str(
        filepath.parent / (filepath.stem + ".1" + filepath.suffix)
    )


def test_get_filepath_existing_tempfile(tmp_path):
    filepath = tmp_path / "test.txt"
    tempfile = tmp_path / "test.txt.matris"
    open(tempfile, "a").close()  # create existing tempfile
    finalpath = get_filepath(filepath, overwrite=False)
    assert str(finalpath) != str(filepath)


def test_get_filepath_overwrite(tmp_path):
    filepath = tmp_path / "test.txt"
    open(filepath, "a").close()  # create existing file
    finalpath = get_filepath(filepath, overwrite=True)
    assert finalpath == filepath


def test_replacement_filename(tmp_path):
    filepath = tmp_path / "test.txt"
    new_path = replacement_filename(filepath)
    assert new_path != filepath
    assert new_path.name.startswith("test")
    assert "".join(new_path.suffixes) == ".1.txt"
