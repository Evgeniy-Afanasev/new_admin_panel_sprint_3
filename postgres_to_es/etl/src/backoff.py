from functools import wraps
from time import sleep

from src.log import log


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Декоратор для повторных попыток выполнения функций с задержкой.
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            while attempt < 10:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    log.error(f'{func.__name__} завершилась с ошибкой: {e}')
                    attempt += 1
                    delay = min(start_sleep_time * (factor ** attempt), border_sleep_time)
                    sleep(delay)

        return inner

    return func_wrapper
