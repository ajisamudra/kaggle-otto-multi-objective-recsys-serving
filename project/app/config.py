import logging
from functools import lru_cache

from pydantic import BaseSettings


log = logging.getLogger("uvicorn")


class Settings(BaseSettings):
    environment: str = "dev"
    reload: bool = 1
    testing: bool = 0


@lru_cache()
def get_settings() -> BaseSettings:
    log.info("Loading config settings from the environment...")
    return Settings()
