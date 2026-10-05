"""Framework-independent request handlers: (status, json-body). FastAPI only wires these up."""
from __future__ import annotations

from ..admin.dashboard import build_stats
from ..search.service import InvalidSearchRequest


def error(status: int, message: str) -> tuple:
    return status, {"error": {"code": status, "message": message}}


def parse_page(raw) -> int:
    if raw is None or raw == "":
        return 1
    if not isinstance(raw, str) or not raw.isascii() or not raw.isdigit() or len(raw) > 4:
        raise InvalidSearchRequest("page must be a positive integer")
    return int(raw)


def handle_search(service, q, page_raw) -> tuple:
    try:
        return 200, service.search(q, parse_page(page_raw))
    except InvalidSearchRequest as e:
        return error(400, str(e))


def handle_login(auth, password, client_key: str) -> tuple:
    status, message, token = auth.login(password, client_key)
    return (status, {"ok": True}, token) if status == 200 else (*error(status, message), None)


def handle_stats(storage, indexer) -> tuple:
    return 200, build_stats(storage, indexer)
