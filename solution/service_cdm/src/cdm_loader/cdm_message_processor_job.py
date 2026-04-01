from logging import Logger


class CdmMessageProcessor:
    def __init__(self, logger: Logger, repository, consumer) -> None:
        self._logger = logger
        self._repository = repository
        self._consumer = consumer

    def run(self) -> None:
        self._logger.info('CDM message processor started')
