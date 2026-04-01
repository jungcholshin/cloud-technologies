import json
from datetime import datetime
from decimal import Decimal
from logging import Logger
from typing import Any, Dict, List, Optional


class DdsMessageProcessor:
    def __init__(self, logger: Logger, repository, consumer, producer) -> None:
        self._logger = logger
        self._repository = repository
        self._consumer = consumer
        self._producer = producer
        self._batch_size = 100
        self._output_topic = "dds-service-output"

    def run(self) -> None:
        self._logger.info("DDS message processor started")

        processed_count = 0

        for _ in range(self._batch_size):
            msg = self._safe_consume()

            if msg is None:
                self._logger.info(
                    "No messages received. Processed %s messages in this run.",
                    processed_count,
                )
                break

            try:
                payload = self._extract_payload(msg)

                if not payload:
                    self._logger.warning("Empty or invalid payload skipped")
                    continue

                result = self._process_message(payload)

                if result is not None:
                    self._send_message(result)

                processed_count += 1

            except Exception as exc:
                self._logger.exception("Error while processing DDS message: %s", exc)

        self._logger.info(
            "DDS message processor finished. Total processed: %s",
            processed_count,
        )

    def _safe_consume(self) -> Optional[Any]:
        try:
            return self._consumer.consume()
        except Exception as exc:
            self._logger.exception("Kafka consume failed: %s", exc)
            return None

    def _extract_payload(self, msg: Any) -> Optional[Dict[str, Any]]:
        raw_value = None

        if msg is None:
            return None

        if isinstance(msg, dict):
            raw_value = msg
        elif hasattr(msg, "value") and callable(msg.value):
            raw_value = msg.value()
        else:
            raw_value = msg

        if raw_value is None:
            return None

        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")

        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
            if not raw_value:
                return None
            return json.loads(raw_value)

        if isinstance(raw_value, dict):
            return raw_value

        self._logger.warning("Unsupported message format: %s", type(raw_value).__name__)
        return None

    def _process_message(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        order_id = self._extract_order_id(payload)
        user_id = self._extract_user_id(payload)
        restaurant_id = self._extract_restaurant_id(payload)
        order_dt = self._extract_order_dt(payload)
        products = self._extract_products(payload)

        username = self._extract_username(payload)
        userlogin = self._extract_userlogin(payload)
        restaurant_name = self._extract_restaurant_name(payload)
        cost = self._extract_cost(payload)
        payment = self._extract_payment(payload)
        status = self._extract_status(payload)

        if not order_id:
            self._logger.warning("Message skipped: order_id not found")
            return None

        if not user_id:
            self._logger.warning("Message skipped: user_id not found")
            return None

        if not restaurant_id:
            self._logger.warning("Message skipped: restaurant_id not found")
            return None

        h_user_pk = self._repository.get_or_create_h_user(user_id)
        h_order_pk = self._repository.get_or_create_h_order(order_id, order_dt)
        h_restaurant_pk = self._repository.get_or_create_h_restaurant(restaurant_id)

        self._repository.insert_l_order_user(h_order_pk, h_user_pk)

        if username or userlogin:
            self._repository.insert_s_user_names(
                h_user_pk,
                username or "",
                userlogin or "",
            )

        if restaurant_name:
            self._repository.insert_s_restaurant_names(
                h_restaurant_pk,
                restaurant_name,
            )

        self._repository.insert_s_order_cost(
            h_order_pk,
            cost,
            payment,
        )

        if status:
            self._repository.insert_s_order_status(
                h_order_pk,
                status,
            )

        output_products: List[Dict[str, Any]] = []

        for product in products:
            product_id = self._extract_product_id(product)
            product_name = self._extract_product_name(product)
            category_name = self._extract_category_name(product)

            if not product_id:
                continue

            h_product_pk = self._repository.get_or_create_h_product(product_id)
            self._repository.insert_l_order_product(h_order_pk, h_product_pk)
            self._repository.insert_l_product_restaurant(h_product_pk, h_restaurant_pk)

            h_category_pk = None
            if category_name:
                h_category_pk = self._repository.get_or_create_h_category(category_name)
                self._repository.insert_l_product_category(h_product_pk, h_category_pk)

            if product_name:
                self._repository.insert_s_product_names(h_product_pk, product_name)

            output_products.append(
                {
                    "product_id": product_id,
                    "product_name": product_name or "",
                    "category_name": category_name or "",
                    "h_product_pk": h_product_pk,
                    "h_category_pk": h_category_pk or "",
                }
            )

        return {
            "order_id": order_id,
            "user_id": user_id,
            "restaurant_id": restaurant_id,
            "order_dt": order_dt.isoformat(),
            "status": status or "",
            "cost": str(cost),
            "payment": str(payment),
            "products": output_products,
        }

    def _send_message(self, payload: Dict[str, Any]) -> None:
        try:
            message = json.dumps(payload, ensure_ascii=False)
            self._producer.produce(self._output_topic, message)
            self._logger.info(
                "DDS output message sent for order_id=%s",
                payload.get("order_id"),
            )
        except Exception as exc:
            self._logger.exception("Failed to send DDS output message: %s", exc)

    # =========================
    # EXTRACTORS
    # =========================

    def _extract_order_id(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("order_id") or payload.get("id") or payload.get("object_id")
        return str(value) if value is not None else None

    def _extract_user_id(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("user_id")
        if value is None and isinstance(payload.get("user"), dict):
            value = payload["user"].get("id")
        return str(value) if value is not None else None

    def _extract_restaurant_id(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("restaurant_id")
        if value is None and isinstance(payload.get("restaurant"), dict):
            value = payload["restaurant"].get("id")
        return str(value) if value is not None else None

    def _extract_order_dt(self, payload: Dict[str, Any]) -> datetime:
        raw_value = (
            payload.get("date")
            or payload.get("order_dt")
            or payload.get("timestamp")
            or payload.get("created_at")
        )

        if not raw_value:
            return datetime.utcnow()

        if isinstance(raw_value, datetime):
            return raw_value

        try:
            normalized = str(raw_value).replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except Exception:
            self._logger.warning(
                "Failed to parse order datetime '%s', using current UTC time",
                raw_value,
            )
            return datetime.utcnow()

    def _extract_products(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        products = payload.get("products")
        if isinstance(products, list):
            return [item for item in products if isinstance(item, dict)]
        return []

    def _extract_product_id(self, product: Dict[str, Any]) -> Optional[str]:
        value = product.get("id") or product.get("product_id")
        return str(value) if value is not None else None

    def _extract_product_name(self, product: Dict[str, Any]) -> Optional[str]:
        value = product.get("name") or product.get("product_name")
        return str(value) if value is not None else None

    def _extract_category_name(self, product: Dict[str, Any]) -> Optional[str]:
        value = product.get("category") or product.get("category_name")
        return str(value) if value is not None else None

    def _extract_username(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("username")
        if value is None and isinstance(payload.get("user"), dict):
            value = payload["user"].get("name")
        return str(value) if value is not None else None

    def _extract_userlogin(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("userlogin")
        if value is None and isinstance(payload.get("user"), dict):
            value = payload["user"].get("login")
        return str(value) if value is not None else None

    def _extract_restaurant_name(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("restaurant_name")
        if value is None and isinstance(payload.get("restaurant"), dict):
            value = payload["restaurant"].get("name")
        return str(value) if value is not None else None

    def _extract_cost(self, payload: Dict[str, Any]) -> Decimal:
        raw_value = payload.get("cost", 0)
        try:
            return Decimal(str(raw_value))
        except Exception:
            return Decimal("0")

    def _extract_payment(self, payload: Dict[str, Any]) -> Decimal:
        raw_value = payload.get("payment", 0)
        try:
            return Decimal(str(raw_value))
        except Exception:
            return Decimal("0")

    def _extract_status(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("status")
        return str(value) if value is not None else None
