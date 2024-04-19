# errors.py

class BadRequest(Exception):
    pass


class NotFound(Exception):
    pass


class DatabaseConnectionError(Exception):
    pass
