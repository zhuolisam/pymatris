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
        dest="max_parallel",
        help="Maximum number of parallel file downloads.",
    )
    parser.add_argument(
        "--max-splits",
        type=int,
        default=5,
        dest="max_splits",
        help="Maximum number of parallel connections per file (only if protocol and server is supported).",
    )
    parser.add_argument(
        "--max-tries",
        type=int,
        default=5,
        dest="max_tries",
        help="Maximum number of download attempt per url.",
    )
    parser.add_argument(
        "--timeouts",
        type=int,
        default=300,
        dest="timeouts",
        help="Maximum timeouts per url.",
    )
    parser.add_argument(
        "--dir",
        type=str,
        default="./",
        help="Directory to which downloaded files are saved.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_const",
        const=True,
        default=False,
        help="Overwrite if file exists. Only one url with the clashing name will overwrite the file.",
    )
    parser.add_argument(
        "--quiet",
        action="store_const",
        const=True,
        default=False,
        dest="quite",
        help="Show progress indicators and file retries if any during download.",
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
        max_tries=args.max_tries,
        timeouts=args.timeouts,
        file_progress=not args.quiet,
        log_level=log_level,
    )

    downloader = Downloader(
        max_parallel=args.max_parallel,
        max_splits=args.max_splits,
        all_progress=not args.quiet,
        overwrite=args.overwrite,
        session_config=config,
    )

    for url in args.urls:
        downloader.enqueue_file(url, path=args.dir)
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
