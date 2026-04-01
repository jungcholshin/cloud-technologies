from logging import Logger


class DdsMessageProcessor:
    def __init__(self, logger: Logger, repository, consumer, producer) -> None:
        self._logger = logger
        self._repository = repository
        self._consumer = consumer
        self._producer = producer

    def run(self) -> None:
        self._logger.info('DDS message processor started')
