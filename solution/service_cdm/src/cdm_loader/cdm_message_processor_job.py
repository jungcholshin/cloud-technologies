import json
from logging import Logger
from typing import Any, Dict, List, Optional


class CdmMessageProcessor:
    def __init__(self, logger: Logger, repository, consumer) -> None:
        self._logger = logger
        self._repository = repository
        self._consumer = consumer
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info("CDM message processor started")

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

                self._process_message(payload)
                processed_count += 1

            except Exception as exc:
                self._logger.exception("Error while processing CDM message: %s", exc)

        self._logger.info(
            "CDM message processor finished. Total processed: %s",
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

    def _process_message(self, payload: Dict[str, Any]) -> None:
        user_id = self._extract_user_id(payload)
        products = self._extract_products(payload)

        if not user_id:
            self._logger.warning("Message skipped: user_id not found")
            return

        for product in products:
            product_id = self._extract_product_id(product)
            product_name = self._extract_product_name(product)
            category_name = self._extract_category_name(product)

            if not product_id:
                continue

            category_id = category_name or "unknown"

            self._repository.insert_user_product_counters(
                user_id,
                product_id,
                product_name or "",
            )

            self._repository.insert_user_category_counters(
                user_id,
                category_id,
                category_name or "",
            )


    def _extract_user_id(self, payload: Dict[str, Any]) -> Optional[str]:
        value = payload.get("user_id")
        return str(value) if value is not None else None

    def _extract_products(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        products = payload.get("products")
        if isinstance(products, list):
            return [item for item in products if isinstance(item, dict)]
        return []

    def _extract_product_id(self, product: Dict[str, Any]) -> Optional[str]:
        value = product.get("product_id") or product.get("id")
        return str(value) if value is not None else None

    def _extract_product_name(self, product: Dict[str, Any]) -> Optional[str]:
        value = product.get("product_name") or product.get("name")
        return str(value) if value is not None else None

    def _extract_category_name(self, product: Dict[str, Any]) -> Optional[str]:
        value = product.get("category_name") or product.get("category")
        return str(value) if value is not None else None
