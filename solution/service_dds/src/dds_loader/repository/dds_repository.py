import uuid
from datetime import datetime
from typing import Optional

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _now(self) -> datetime:
        return datetime.utcnow()

    def get_h_user_pk(self, user_id: str) -> Optional[str]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h_user_pk
                    FROM dds.h_user
                    WHERE user_id = %s;
                    """,
                    (user_id,)
                )
                row = cur.fetchone()
                return str(row[0]) if row else None

    def get_h_order_pk(self, order_id: str) -> Optional[str]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h_order_pk
                    FROM dds.h_order
                    WHERE order_id = %s;
                    """,
                    (order_id,)
                )
                row = cur.fetchone()
                return str(row[0]) if row else None

    def get_h_product_pk(self, product_id: str) -> Optional[str]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h_product_pk
                    FROM dds.h_product
                    WHERE product_id = %s;
                    """,
                    (product_id,)
                )
                row = cur.fetchone()
                return str(row[0]) if row else None

    def get_h_restaurant_pk(self, restaurant_id: str) -> Optional[str]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h_restaurant_pk
                    FROM dds.h_restaurant
                    WHERE restaurant_id = %s;
                    """,
                    (restaurant_id,)
                )
                row = cur.fetchone()
                return str(row[0]) if row else None

    def get_h_category_pk(self, category_name: str) -> Optional[str]:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT h_category_pk
                    FROM dds.h_category
                    WHERE category_name = %s;
                    """,
                    (category_name,)
                )
                row = cur.fetchone()
                return str(row[0]) if row else None

    def get_or_create_h_user(self, user_id: str) -> str:
        h_user_pk = self.get_h_user_pk(user_id)
        if h_user_pk:
            return h_user_pk

        new_pk = str(uuid.uuid4())
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (new_pk, user_id, self._now(), 'kafka')
                )
        return new_pk

    def get_or_create_h_order(self, order_id: str, order_dt: datetime) -> str:
        h_order_pk = self.get_h_order_pk(order_id)
        if h_order_pk:
            return h_order_pk

        new_pk = str(uuid.uuid4())
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (new_pk, order_id, order_dt, self._now(), 'kafka')
                )
        return new_pk

    def get_or_create_h_product(self, product_id: str) -> str:
        h_product_pk = self.get_h_product_pk(product_id)
        if h_product_pk:
            return h_product_pk

        new_pk = str(uuid.uuid4())
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (new_pk, product_id, self._now(), 'kafka')
                )
        return new_pk

    def get_or_create_h_restaurant(self, restaurant_id: str) -> str:
        h_restaurant_pk = self.get_h_restaurant_pk(restaurant_id)
        if h_restaurant_pk:
            return h_restaurant_pk

        new_pk = str(uuid.uuid4())
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (new_pk, restaurant_id, self._now(), 'kafka')
                )
        return new_pk

    def get_or_create_h_category(self, category_name: str) -> str:
        h_category_pk = self.get_h_category_pk(category_name)
        if h_category_pk:
            return h_category_pk

        new_pk = str(uuid.uuid4())
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (new_pk, category_name, self._now(), 'kafka')
                )
        return new_pk
