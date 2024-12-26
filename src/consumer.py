import logging
import sys
from datetime import datetime
from time import sleep
from typing import Annotated, Dict, List, Union, cast

import redis
import redis.exceptions

from typing_extensions import Doc

from src.settings import init_logging

from .common import DTO, MessageResult, MessageType

logger = logging.getLogger('consumer')

XReadGroupResponse = Annotated[
    list,
    Doc('example: [[b"stream_name1", Tuple[RedisMessage, ...]], [b"stream_name2", Tuple[RedisMessage, ...]]]'),
]
RedisMessage = Annotated[tuple, Doc('example: (b"message_id", "{**message_content}")')]


class MessageDTO(DTO):
    id: str
    type: MessageType
    message: str


class HandleMessageResultDTO(DTO):
    message_id: str
    message_result: Union[MessageResult, None]


def _parse_message(message: RedisMessage) -> MessageDTO:
    deserialized_data: Dict[str, str] = {}

    for key, value in cast(Dict[bytes, bytes], message[1]).items():
        deserialized_data[key.decode()] = value.decode()

    return MessageDTO(
        id=cast(bytes, message[0]).decode(),
        type=MessageType[deserialized_data['type']],
        message=deserialized_data['message'],
    )


def _generate_mock_result(message: MessageDTO) -> MessageResult:
    return f'{message.type}__{datetime.now()}'


def _handle_message(message: MessageDTO) -> HandleMessageResultDTO:
    if message.type == MessageType.EVENT:
        message_result = None

    if message.type == MessageType.ASYNC_COMMAND:
        message_result = None

    if message.type == MessageType.SYNC_COMMAND:
        message_result = _generate_mock_result(message=message)

    if message.type == MessageType.QUERY:
        message_result = _generate_mock_result(message=message)

    return HandleMessageResultDTO(message_id=message.id, message_result=message_result)


def main():
    consumer_name = f'consumer_{sys.argv[-1]}'

    logger.info(f'Hi, I am consumer "{consumer_name}"')

    redis_client = redis.Redis(host='localhost', port=16379)

    groupname = 'cqrs_consumers_group'
    stream_name = 'cqrs_stream'

    try:
        redis_client.xgroup_create(name=stream_name, groupname=groupname, id=0, mkstream=True)
    except redis.exceptions.RedisError as e:
        if 'BUSYGROUP' in str(e):
            pass
        else:
            raise e

    while True:

        response: XReadGroupResponse = redis_client.xreadgroup(
            groupname=groupname,
            consumername=consumer_name,
            streams={stream_name: '>'},
            count=10,
        )

        failed_messages: List[MessageDTO] = []
        successfully_handled_messages: List[MessageDTO] = []
        handle_message_results: List[HandleMessageResultDTO] = []

        if response:
            logger.debug('there is messages in response!')
            for message in response[0][1]:
                message = cast(RedisMessage, message)

                message = _parse_message(message=message)

                try:
                    handle_message_result = _handle_message(message=message)
                except Exception:
                    failed_messages.append(message)
                    continue

                logger.debug(f'{handle_message_result.message_id} was handled!')

                successfully_handled_messages.append(message)
                handle_message_results.append(handle_message_result)

            if successfully_handled_messages:
                redis_client.xack(
                    stream_name,
                    groupname,
                    *[message.id for message in successfully_handled_messages],
                )

                logger.debug('successfully_handled_messages were acknowledged')
                successfully_handled_messages = []

                if handle_message_results:
                    # TODO: rewrite to bulk operation if exists
                    for handle_message_result in handle_message_results:
                        if handle_message_result.message_result is None:
                            continue

                        redis_client.lpush(
                            handle_message_result.message_id,
                            handle_message_result.message_result,
                        )
                        logger.debug(
                            'message_result for message with id' +    # noqa
                            f' = {handle_message_result.message_id} were pushed to redis',
                        )

                    handle_message_results = []

            if failed_messages:
                redis_client.xack(
                    stream_name,
                    groupname,
                    *[message.id for message in failed_messages],
                )

                for message in failed_messages:
                    redis_client.xadd(name=stream_name, fields=message.data)

                failed_messages = []
                logger.debug('failed_messages were returned to original stream')

        else:
            logger.debug('there is no messages in response, we will try later...')
            sleep(3)


if __name__ == '__main__':
    init_logging()
    main()
