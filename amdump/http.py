import time
import logging
import functools

import requests


LOGGER = logging.getLogger(__name__)

MAX_RETRIES = 4
BACKOFF_FACTOR = 1.9


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


class retry_gracefully:
    """Produz decorador que torna o objeto decorado resiliente às exceções dos
    tipos informados em `exc_list`. Tenta no máximo `max_retries` vezes com
    intervalo exponencial entre as tentativas.
    """

    def __init__(
        self,
        max_retries=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        exc_list=(RetryableError,),
    ):
        self.max_retries = int(max_retries)
        self.backoff_factor = float(backoff_factor)
        self.exc_list = tuple(exc_list)

    def _sleep(self, seconds):
        time.sleep(seconds)

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except self.exc_list as exc:
                    if retry <= self.max_retries:
                        wait_seconds = self.backoff_factor ** retry
                        LOGGER.info(
                            'could not get the result for "%s" with *args "%s" '
                            'and **kwargs "%s". retrying in %s seconds '
                            "(retry #%s): %s",
                            func.__qualname__,
                            args,
                            kwargs,
                            str(wait_seconds),
                            retry,
                            exc,
                        )
                        self._sleep(wait_seconds)
                        retry += 1
                    else:
                        raise

        return wrapper


@retry_gracefully()
def get(url: str, timeout: float = 2) -> bytes:
    try:
        response = requests.get(url, timeout=timeout)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
        raise RetryableError(exc) from exc
    except (
        requests.exceptions.InvalidSchema,
        requests.exceptions.MissingSchema,
        requests.exceptions.InvalidURL,
    ) as exc:
        raise NonRetryableError(exc) from exc
    else:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            if 400 <= exc.response.status_code < 500:
                raise NonRetryableError(exc) from exc
            elif 500 <= exc.response.status_code < 600:
                raise RetryableError(exc) from exc
            else:
                raise

    return response.content

