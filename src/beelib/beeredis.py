import redis as _redis

from beelib.beeretry import retry_with_backoff, RetryingProxy

# ConnectionError cobreix un ConnectionResetError (failover/rotacio de node de
# Redis); TimeoutError cobreix un servidor que no respon pero no talla la
# connexio.
REDIS_TRANSIENT_EXCEPTIONS = (_redis.exceptions.ConnectionError, _redis.exceptions.TimeoutError)


def connect_redis(cfg: dict) -> _redis.Redis:
    """Connecta a Redis (client directe, o via Sentinel si cfg['connection']
    conte 'sentinels'), verifica la connexio amb un ping reintentat, i
    retorna un client on QUALSEVOL comanda (lpush, brpop, setnx...) reintenta
    sola les excepcions transitories -- no cal (ni es pot) fer-la servir
    "sense retry" en cap crida posterior.

    El client de redis-py es lazy (no connecta de veritat fins al primer
    command), aixi que sense el ping inicial un tall de xarxa o un failover no
    es detectaria fins la primera crida real feta pel codi cridant.
    """
    sentinel_cfg = cfg.get('connection', {})
    if 'sentinels' in sentinel_cfg:
        from redis.sentinel import Sentinel
        conn_kwargs = {k: v for k, v in cfg.items() if k not in ('connection', 'master_name')}
        sentinel = Sentinel(sentinel_cfg['sentinels'], **conn_kwargs)
        conn = sentinel.master_for(cfg['master_name'], **conn_kwargs)
    else:
        conn = _redis.Redis(**cfg)
    retry_with_backoff(conn.ping, exceptions=REDIS_TRANSIENT_EXCEPTIONS)
    return RetryingProxy(conn, REDIS_TRANSIENT_EXCEPTIONS)
