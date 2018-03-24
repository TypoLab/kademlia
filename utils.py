import asyncio
from itertools import chain
from typing import Callable, Awaitable, TypeVar


T = TypeVar('T')
AsyncFunc = Callable[..., Awaitable[T]]


def limited(sem: asyncio.Semaphore, func: AsyncFunc) -> AsyncFunc:
    '''Limit the number of coroutines running at the same time.'''
    async def wrapper(*args, **kwargs) -> T:
        with (await sem):
            return await func(*args, **kwargs)

    return wrapper


def flatten(nested: list) -> list:
    return list(chain(*nested))
