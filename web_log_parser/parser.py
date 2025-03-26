from datetime import datetime
import logging
import os
import re
import sqlite3

from web_log_parser.request_filtering import is_record_important
from web_log_parser.inventory import LogRecord

logger = logging.getLogger(__name__)


class Parser:
    _TABLE_NAME = "logs"

    def __init__(
        self, log_path: str, db_path: str, last_cursor_position_file_path: str
    ):
        self.log_path = log_path
        self.sqlite_db_path = db_path
        self.last_cursor_position_file_path = last_cursor_position_file_path

    def parse(self):
        if not os.path.exists(self.log_path):
            logging.error(f"Log file does not exist: {self.log_path}")
            self._update_last_cursor_position(0)
            return

        last_cursor_position = self._get_last_cursor_position()
        new_cursor_position = self._get_new_cursor_position()

        if new_cursor_position < last_cursor_position:
            # log apparently truncated, start from the beginning
            last_cursor_position = 0

        lines = self._read_log_lines(last_cursor_position, new_cursor_position)
        new_cursor_position = self._get_new_cursor_position()  # in case it grew

        # convert each of the lines we've read into a record object
        records = [self._parse_line(line) for line in lines]
        records = [record for record in records if record is not None]

        # filter out the records that we don't want to insert
        records = [
            record for record in records if self._record_should_be_inserted(record)
        ]

        self._update_last_cursor_position(new_cursor_position)

        # bulk insert the records into the database
        self._bulk_insert(records)

    def _update_last_cursor_position(self, new_cursor_position: int):
        with open(self.last_cursor_position_file_path, "w") as f:
            f.write(str(new_cursor_position))

    def _get_new_cursor_position(self):
        return os.path.getsize(self.log_path)

    def _get_last_cursor_position(self):
        if not os.path.exists(self.last_cursor_position_file_path):
            return 0
        with open(self.last_cursor_position_file_path, "r") as f:
            return int(f.read())

    def _read_log_lines(self, last_cursor_position: int, new_cursor_position: int):
        """Read the file as lines starting from the last cursor position in bytes
        till the new cursor position in bytes.

        """
        with open(self.log_path, "r") as f:
            f.seek(last_cursor_position)
            return f.readlines()

    def _parse_line(self, line: str) -> LogRecord | None:
        """Parse a single line from the log file and return a LogRecord object."""
        log_pattern = re.compile(
            r"(?P<ip>\S+) - (?P<host>\S+) \[(?P<timestamp>[^\]]+)\] "
            r'"(?P<method>\S+) (?P<path>\S+) \S+" (?P<status>\d+) (?P<bytes_sent>\d+) '
            r'(?P<request_time>[\d.]+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"'
        )

        match = log_pattern.match(line)
        if not match:
            logging.error(f"Line does not match expected format: {line}")
            return None

        data = match.groupdict()

        # Parse timestamp into a UNIX timestamp
        timestamp = datetime.strptime(
            data["timestamp"], "%d/%b/%Y:%H:%M:%S %z"
        ).timestamp()

        # Create and return a LogRecord object
        return LogRecord(
            ip=data["ip"],
            host=data["host"],
            timestamp=timestamp,
            method=data["method"],
            path=data["path"],
            status=int(data["status"]),
            bytes_sent=int(data["bytes_sent"]),
            referer=data["referer"],
            user_agent=data["user_agent"],
            response_body_size=data[
                "bytes_sent"
            ],  # Assuming bytes_sent is the response body size
            request_time=float(data["request_time"]),
            processed_timestamp=datetime.now().timestamp(),
        )

    def _record_should_be_inserted(self, record: LogRecord) -> bool:
        return is_record_important(record)

    def _bulk_insert(self, records: list[LogRecord]):
        """Insert a list of LogRecord objects into the database."""
        if not records:
            return  # No records to insert

        # Define the table creation query
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self._TABLE_NAME} (
                ip TEXT,
                host TEXT,
                timestamp REAL,
                method TEXT,
                path TEXT,
                status INTEGER,
                bytes_sent INTEGER,
                referer TEXT,
                user_agent TEXT,
                response_body_size TEXT,
                request_time REAL,
                processed_timestamp REAL
            )
        """

        # Define the insert query
        insert_query = f"""
            INSERT INTO {self._TABLE_NAME} (
                ip, host, timestamp, method, path, status, bytes_sent,
                referer, user_agent, response_body_size, request_time, processed_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Connect to the SQLite database
        connection = sqlite3.connect(self.sqlite_db_path)
        try:
            with connection:
                # Create the table if it doesn't exist
                connection.execute(create_table_query)

                # Prepare the data for bulk insertion
                data = [
                    (
                        record.ip,
                        record.host,
                        record.timestamp,
                        record.method,
                        record.path,
                        record.status,
                        record.bytes_sent,
                        record.referer,
                        record.user_agent,
                        record.response_body_size,
                        record.request_time,
                        record.processed_timestamp,
                    )
                    for record in records
                ]

                # Perform the bulk insert
                connection.executemany(insert_query, data)
        finally:
            connection.close()

        logger.info(f"Inserted {len(records)} records into the database")
