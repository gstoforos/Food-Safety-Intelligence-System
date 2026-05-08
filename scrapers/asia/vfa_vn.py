"""VFA (VN) food safety scraper — uses Gemini for HTML extraction.

TLS workaround (added 2026-05-07):
vfa.gov.vn negotiates TLS using a Diffie-Hellman key smaller than 1024 bits.
Modern OpenSSL refuses this by default at SECLEVEL=2 with the error
"[SSL: DH_KEY_TOO_SMALL] dh key too small". Lowering OpenSSL's security
level to SECLEVEL=1 lets the handshake complete.

We do this surgically — mounting a custom adapter ONLY for the
`https://vfa.gov.vn` URL prefix on this scraper's session. All other
hosts (and all other scrapers) continue to use the default secure
context. Each scraper has its own session, so the lowered security
level cannot leak to neighbours.
"""
from __future__ import annotations

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.util.ssl_ import create_urllib3_context

from scrapers._base import GenericGeminiScraper


class _LegacyTLSAdapter(HTTPAdapter):
    """HTTPAdapter that uses an OpenSSL context at SECLEVEL=1 so it can
    negotiate with servers offering small DH parameters or older ciphers.
    Limited blast radius: only mounted on the VFA host below.
    """

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers="DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers="DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().proxy_manager_for(*args, **kwargs)


class VFAScraper(GenericGeminiScraper):
    AGENCY = "VFA (VN)"
    COUNTRY = "Vietnam"
    INDEX_URLS = ['https://vfa.gov.vn/canh-bao/']
    LANGUAGE = "vi"

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        # Mount the SECLEVEL=1 adapter only for the VFA host.
        # Retry/backoff config mirrors scrapers._base.make_session()'s
        # defaults so we don't lose retry behaviour for this host.
        retry = Retry(
            total=3, read=3, connect=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
            raise_on_status=False,
        )
        self.session.mount(
            "https://vfa.gov.vn",
            _LegacyTLSAdapter(
                max_retries=retry,
                pool_connections=20,
                pool_maxsize=20,
            ),
        )
