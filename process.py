import argparse
import logging
from web_log_parser.parser import Parser


def main(path_to_observed_log, path_to_db_file, path_to_cursor_position_file, log_file):
    # Configure logging
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("Starting log processing")
    print(f"Log file: {path_to_observed_log}")
    print(f"Database file: {path_to_db_file}")
    print(f"Cursor position file: {path_to_cursor_position_file}")
    logging.info("Initialized parser with provided paths")
    parser = Parser(
        log_path=path_to_observed_log,
        db_path=path_to_db_file,
        last_cursor_position_file_path=path_to_cursor_position_file,
    )

    parser.parse()
    logging.info("Finished log processing")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process web log files.")
    parser.add_argument(
        "path_to_observed_log", help="Path to the log file to be observed"
    )
    parser.add_argument("path_to_db_file", help="Path to the database file")
    parser.add_argument(
        "path_to_cursor_position_file", help="Path to the cursor position file"
    )
    parser.add_argument("log_file", help="Path to the log file for logging")
    args = parser.parse_args()

    main(
        args.path_to_observed_log,
        args.path_to_db_file,
        args.path_to_cursor_position_file,
        args.log_file,
    )
