import logging
from typing import Annotated, cast

from redis import Redis

from typing_extensions import Doc

from src.settings import init_logging

from .common import MessageResult, MessageType

logger = logging.getLogger('publisher')

BLPOPResponse = Annotated[tuple, Doc('examle: Tuple[b"message_id", b"MessageResult"]')]


class EventPublisher:
    def __init__(self, redis_client: Redis):
        self._redis_client = redis_client

    def send_message(self, message_body: str) -> None:
        self._redis_client.xadd(
            name='cqrs_stream',
            fields={
                'type': MessageType.EVENT.value,
                'message': message_body,
            },
        )


class CommandPublisher:
    def __init__(self, redis_client: Redis):
        self._redis_client = redis_client

    def async_send_message(self, message_body: str) -> None:
        self._redis_client.xadd(
            name='cqrs_stream',
            fields={
                'type': MessageType.ASYNC_COMMAND.value,
                'message': message_body,
            },
        )

    def sync_send_message(self, message_body: str) -> MessageResult:
        message_id = self._redis_client.xadd(
            name='cqrs_stream',
            fields={
                'type': MessageType.SYNC_COMMAND.value,
                'message': message_body,
            },
        )

        # TODO: obtain with transaction
        response: BLPOPResponse = self._redis_client.blpop(keys=[message_id], timeout=60)
        self._redis_client.delete(message_id)

        return cast(bytes, response[1]).decode()


class QueryPublisher:
    def __init__(self, redis_client: Redis):
        self._redis_client = redis_client

    def send_message(self, message_body: str) -> MessageResult:
        message_id = self._redis_client.xadd(
            name='cqrs_stream',
            fields={
                'type': MessageType.QUERY.value,
                'message': message_body,
            },
        )

        # TODO: obtain with transaction
        response: BLPOPResponse = self._redis_client.blpop(keys=[message_id], timeout=60)
        self._redis_client.delete(message_id)

        return cast(bytes, response[1]).decode()


class Testing:
    def __init__(self, redis_client: Redis):
        self._redis_client = redis_client

    def send_event(self) -> None:
        publisher = EventPublisher(redis_client=self._redis_client)
        publisher.send_message(message_body='hello world')
        logging.debug('event was sended')

    def send_async_command(self) -> None:
        publisher = CommandPublisher(redis_client=self._redis_client)
        publisher.async_send_message(message_body='hello world')
        logging.debug('async command was sended')

    def send_sync_command(self) -> None:
        publisher = CommandPublisher(redis_client=self._redis_client)
        message_result = publisher.sync_send_message(message_body='hello world')
        logging.debug(f'sync command was sended, message_result = "{message_result}"')

    def send_query(self) -> None:
        publisher = QueryPublisher(redis_client=self._redis_client)
        message_result = publisher.send_message(message_body='hello world')
        logging.debug(f'sync command was sended, message_result = "{message_result}"')


def main():
    testing = Testing(redis_client=Redis(host='localhost', port=16379))

    # testing.send_event()
    # testing.send_event()
    # testing.send_event()

    # testing.send_async_command()
    # testing.send_async_command()
    # testing.send_async_command()

    # testing.send_sync_command()
    # testing.send_sync_command()
    # testing.send_sync_command()

    testing.send_query()
    testing.send_query()
    testing.send_query()


if __name__ == '__main__':
    init_logging()
    main()
