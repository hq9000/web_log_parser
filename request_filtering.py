from web_log_parser.inventory import LogRecord


def is_record_important(record: LogRecord) -> bool:
    non_important_suffixes = [
        ".ico",
        ".css",
        ".js",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".svg",
        ".woff",
        ".ttf",
        ".eot",
        ".otf",
        ".map",
    ]

    if record.path.endswith(tuple(non_important_suffixes)):
        return False

    if record.status >= 400:
        return False

    return True
