from dataclasses import dataclass


@dataclass
class LogRecord:
    ip: str
    host: str
    timestamp: int
    method: str
    path: str
    status: int
    bytes_sent: int
    referer: str
    user_agent: str
    response_body_size: str
    request_time: float
    processed_timestamp: int
    session_id: str = None
