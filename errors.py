# errors.py

class BadRequest(Exception):  # 400
    pass


class NotFound(Exception):  # 404
    pass


class DatabaseConnectionError(Exception):  # 503
    pass


class NoFileProvided(Exception):  # 400
    pass


class FileNameExists(Exception):  # 409
    pass


class ExceedsFileSizeLimit(Exception):  # 413
    pass


class InternalServerError(Exception):  # 500
    pass


class NotImplement(Exception):  # 501
    pass
