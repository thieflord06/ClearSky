# errors.py

class BadRequest(Exception):
    pass


class NotFound(Exception):
    pass


class DatabaseConnectionError(Exception):
    pass


class NoFileProvided(Exception):
    pass


class FileNameExists(Exception):
    pass
