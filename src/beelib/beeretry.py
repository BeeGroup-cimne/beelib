import logging
import random
import time

logger = logging.getLogger(__name__)


def retry_with_backoff(fn, *args, exceptions=Exception, attempts=10, base_delay=5,
                        max_delay=60, factor=2, jitter=0.2, on_retry=None, **kwargs):
    """Crida fn(*args, **kwargs) reintentant si llenca una de `exceptions`.

    Backoff exponencial (comenca a base_delay, es multiplica per factor a cada
    intent, capat a max_delay) amb jitter perque diversos jobs que fallen alhora
    no reintentin tots exactament al mateix instant.

    Amb els valors per defecte (10 intents, 5s->60s) el temps total d'espera
    abans de rendir-se es d'uns 7 minuts. Es tria expressament per sobre dels
    ~4m30s que va trigar un node de Kafka a recuperar-se en un reinici de node
    real (2026-09-15): prou marge per sobreviure un failover/rotacio de node de
    Redis, Neo4j, Kafka o InfluxDB sense allargar indefinidament un Job quan el
    servei esta realment caigut (amb 10 intents encara acaba fallant, nomes que
    mes tard).
    """
    delay = base_delay
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except exceptions as e:
            if attempt == attempts:
                raise
            sleep_for = min(delay, max_delay) * (1 + random.uniform(0, jitter))
            logger.warning(
                "retry_with_backoff: attempt %d/%d failed (%s: %s), retrying in %.1fs",
                attempt, attempts, type(e).__name__, e, sleep_for,
            )
            if on_retry:
                on_retry(attempt, e)
            time.sleep(sleep_for)
            delay = min(delay * factor, max_delay)


class RetryingProxy:
    """Embolica un client/connexio (redis, driver...) perque qualsevol metode
    que s'hi cridi (lpush, brpop, setnx...) reintenti sol les excepcions
    transitories indicades, amb retry_with_backoff.

    Aixi la resiliencia queda lligada al punt on es crea la connexio (veure
    beeredis.connect_redis) i no al codi que la fa servir despres: cap crida
    feta a traves d'aquest proxy es pot "oblidar" de reintentar, perque no hi
    ha cap manera de fer-la servir sense passar per aqui.

    Nomes te sentit quan els metodes retornen dades planes (strings, llistes,
    None...) i no un altre objecte que calgui embolicar igual (com fa
    neo4j amb driver.session() -- per aixo Neo4j te el seu propi wrapper a
    beeneo4j en lloc de fer servir aquest de proposit general).
    """

    def __init__(self, target, exceptions, **retry_kwargs):
        self._target = target
        self._exceptions = exceptions
        self._retry_kwargs = retry_kwargs

    def __getattr__(self, name):
        attr = getattr(self._target, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            return retry_with_backoff(attr, *args, exceptions=self._exceptions, **self._retry_kwargs, **kwargs)
        return call
