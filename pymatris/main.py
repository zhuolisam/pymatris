import sys
import argparse
from pymatris import Downloader, SessionConfig


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="pymatris: Parallel download manager for HTTP/HTTPS/FTP/SFTP protocols."
    )
    parser.add_argument(
        "urls",
        metavar="URLS",
        type=str,
        nargs="+",
        help="URLs of files to be downloaded.",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=5,
        help="Maximum number of parallel file downloads.",
    )
    parser.add_argument(
        "--max-splits",
        type=int,
        default=5,
        help="Maximum number of parallel connections per file (only used if supported by the server).",
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="./",
        help="Directory to which downloaded files are saved.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_const",
        const=True,
        default=False,
        help="Overwrite if the file exists. Only one matched file will be overwritten",
    )
    parser.add_argument(
        "--no-progress",
        action="store_const",
        const=True,
        default=False,
        dest="no_progress",
        help="Show progress indicators during download.",
    )
    parser.add_argument(
        "--no-file-progress",
        action="store_const",
        const=True,
        default=False,
        dest="no_file_progress",
        help="Show progress bar for each file.",
    )
    parser.add_argument(
        "--show-errors",
        action="store_const",
        const=True,
        default=False,
        dest="show_errors",
        help="Show failed downloads with its errors to stderr.",
    )
    parser.add_argument(
        "--verbose",
        action="store_const",
        const=True,
        default=False,
        help="Log debugging output while transferring the files.",
    )
    args = parser.parse_args(args)

    # If no arguments are provided, print the help menu
    if not args:
        parser.print_help()
        exit()

    return args


def run_pymatris(args):
    log_level = "DEBUG" if args.verbose else None
    config = SessionConfig(
        file_progress=not args.no_file_progress,
        log_level=log_level,
    )

    downloader = Downloader(
        max_parallel=args.max_parallel,
        max_splits=args.max_splits,
        all_progress=not args.no_progress,
        overwrite=args.overwrite,
        session_config=config,
    )

    for url in args.urls:
        downloader.enqueue_file(url, path=args.directory)
    results = downloader.download()

    for i in results:
        print(i + " downloaded")

    if args.show_errors:
        err_str = ""
        if results.errors:
            err_str += "\nErrors:\n"
            for err in results.errors:
                err_str += f"{repr(err)}"
        if err_str:
            print(err_str, file=sys.stderr)
            sys.exit(1)

    sys.exit(0)


def main():
    args = parse_args(sys.argv[1:])
    run_pymatris(args)


if __name__ == "__main__":
    main()
