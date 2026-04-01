from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
