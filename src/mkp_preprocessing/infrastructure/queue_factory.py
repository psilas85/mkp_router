#mkp_router/src/mkp_preprocessing/infrastructure/queue_factory.py

import os
from redis import Redis
from rq import Queue

# =========================
# Configuração base
# =========================

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

DEFAULT_TIMEOUT = 36000   # 10h (comentário corrigido)
LONG_TIMEOUT = 36000      # 10h (jobs pesados)

# =========================
# Redis singleton
# =========================

_redis = None

def get_redis():
    global _redis
    if _redis is None:
        _redis = Redis.from_url(REDIS_URL)
    return _redis

# =========================
# Factory genérica
# =========================

def get_queue(
    name: str,
    timeout: int | None = None,
) -> Queue:
    return Queue(
        name=name,
        connection=get_redis(),
        default_timeout=timeout or DEFAULT_TIMEOUT,
    )

# =========================
# Atalhos
# =========================

def fila_nominatim():
    return get_queue("mkp_nominatim")

def fila_google():
    return get_queue("mkp_google")

def fila_viacep():
    return get_queue("mkp_viacep")

def fila_resultados():
    return get_queue("mkp_resultados", timeout=LONG_TIMEOUT)

def fila_geocode():
    return get_queue("mkp_geocode", timeout=LONG_TIMEOUT)

def fila_preprocessing():
    return get_queue("mkp_preprocessing", timeout=LONG_TIMEOUT)
