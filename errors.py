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


class ExceedsFileSizeLimit(Exception):
    pass


class InternalServerError(Exception):
    pass
