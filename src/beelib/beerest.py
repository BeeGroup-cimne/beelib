import requests

from beelib.beeretry import RetryingProxy

# ConnectionError cobreix talls TCP/TLS a mig handshake o a mig resposta
# (connection reset, connection aborted...); Timeout cobreix un servidor que
# no respon pero no talla la connexio; ChunkedEncodingError cobreix una
# resposta streamejada tallada a mitges.
HTTP_TRANSIENT_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def session_with_retries(exceptions=HTTP_TRANSIENT_EXCEPTIONS, **retry_kwargs) -> RetryingProxy:
    """Retorna una requests.Session on QUALSEVOL metode (get, post...)
    reintenta sol les excepcions transitories indicades -- pensat per clients
    d'APIs externes (Ixon, Dexma...) que fan moltes crides HTTP seguides,
    on un sol tall de connexio no hauria de fer petar tot el job.

    Igual que beeredis.connect_redis, el retry queda lligat a la sessio i no
    al codi que la fa servir despres -- cap crida feta a traves d'aquesta
    sessio es pot "oblidar" de reintentar.
    """
    return RetryingProxy(requests.Session(), exceptions, **retry_kwargs)
