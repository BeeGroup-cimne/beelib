from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

from beelib.beeretry import retry_with_backoff

NEO4J_TRANSIENT_EXCEPTIONS = (ServiceUnavailable, SessionExpired, TransientError)


class _RetryingSession:
    """Sessio de Neo4j on `run()` reintenta sol les excepcions transitories.
    Delega __enter__/__exit__/close a la sessio real perque es pugui fer
    servir exactament igual amb `with driver.session() as s: s.run(...)`."""

    def __init__(self, session, exceptions):
        self._session = session
        self._exceptions = exceptions

    def __enter__(self):
        self._session.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._session.__exit__(exc_type, exc_val, exc_tb)

    def run(self, *args, **kwargs):
        return retry_with_backoff(self._session.run, *args, exceptions=self._exceptions, **kwargs)

    def close(self):
        self._session.close()


class _RetryingDriver:
    """Driver de Neo4j on session() retorna una _RetryingSession -- aixi
    qualsevol `.run()` fet a traves seu reintenta sol, sense que el codi
    cridant hagi de fer res diferent del que ja feia."""

    def __init__(self, driver, exceptions):
        self._driver = driver
        self._exceptions = exceptions

    def session(self, *args, **kwargs):
        return _RetryingSession(self._driver.session(*args, **kwargs), self._exceptions)

    def verify_connectivity(self, *args, **kwargs):
        return self._driver.verify_connectivity(*args, **kwargs)

    def close(self):
        self._driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def connect_neo4j(config: dict):
    """Crea el driver de Neo4j, en verifica la connectivitat amb un retry, i
    el retorna embolicat perque qualsevol `.session()...run()` fet a traves
    seu reintenti sol les excepcions transitories -- no cal (ni es pot) fer
    servir la connexio "sense retry" en cap query posterior.

    GraphDatabase.driver() es lazy (no obre cap connexio fins la primera
    sessio/query), aixi que sense la verificacio inicial un tall de xarxa o un
    reinici del node no es detectaria fins la primera query real.

    Accepta el dict de config tal com surt del JSON (p.ex. {"uri":...,
    "auth": [user, password], "encrypted": ...}) -- 'auth' com a llista es
    normalitza a tupla, que es el que exigeix GraphDatabase.driver().
    """
    if isinstance(config.get('auth'), list):
        config = {**config, 'auth': tuple(config['auth'])}
    driver = GraphDatabase.driver(**config)
    retry_with_backoff(driver.verify_connectivity, exceptions=NEO4J_TRANSIENT_EXCEPTIONS)
    return _RetryingDriver(driver, NEO4J_TRANSIENT_EXCEPTIONS)
