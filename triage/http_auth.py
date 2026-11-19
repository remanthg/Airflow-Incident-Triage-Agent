import os
from pathlib import Path
from urllib.parse import urlsplit

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Receive, Scope, Send

from .airflow_logs import http_url
from .log_server import create_server

LOG_READ_SCOPE = "logs:read"


class AccessTokenVerifier:
    def __init__(self, public_key: RSAPublicKey, issuer: str, resource: str):
        self.public_key = public_key
        self.issuer = issuer
        self.resource = resource

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            decoded = jwt.decode_complete(
                token, self.public_key, algorithms=["RS256"], issuer=self.issuer,
                audience=self.resource,
                options={"require": ["exp", "iss", "aud", "sub", "client_id", "iat", "jti"]},
            )
        except (jwt.InvalidTokenError, TypeError, ValueError, OverflowError):
            return None
        typ = decoded["header"].get("typ", "")
        claims = decoded["payload"]
        client_id = claims["client_id"]
        scope = claims.get("scope", "")
        if (not isinstance(typ, str) or typ.lower() not in {"at+jwt", "application/at+jwt"}
                or not isinstance(client_id, str) or not client_id or not isinstance(scope, str)):
            return None
        return AccessToken(token=token, client_id=client_id, subject=claims["sub"],
                           scopes=scope.split(), expires_at=int(claims["exp"]), resource=self.resource)


class ScopeChallenge:
    """Add the scope hint to the SDK's insufficient_scope challenge."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def add_hint(message):
            if message["type"] == "http.response.start" and message["status"] == 403:
                headers = MutableHeaders(scope=message)
                challenge = headers.get("www-authenticate", "")
                if 'error="insufficient_scope"' in challenge and ', scope=' not in challenge:
                    headers["www-authenticate"] = f'{challenge}, scope="{LOG_READ_SCOPE}"'
            await send(message)

        await self.app(scope, receive, add_hint)


def create_http_app(source: str, *, issuer: str, resource: str, public_key: RSAPublicKey):
    http_url(issuer, "TRIAGE_OAUTH_ISSUER")
    http_url(resource, "TRIAGE_MCP_RESOURCE_URL")
    if urlsplit(issuer).scheme != "https":
        raise ValueError("TRIAGE_OAUTH_ISSUER must use HTTPS.")
    if urlsplit(resource).path != "/mcp":
        raise ValueError("TRIAGE_MCP_RESOURCE_URL must use the /mcp endpoint.")
    auth = AuthSettings(issuer_url=issuer, resource_server_url=resource,
                        required_scopes=[LOG_READ_SCOPE], validate_token_resource=True)
    server = create_server(source, auth=auth, token_verifier=AccessTokenVerifier(public_key, issuer, resource),
                           stateless_http=True, json_response=True)
    app = server.streamable_http_app()
    app.add_middleware(ScopeChallenge)
    return app


def configured_http_app(source: str):
    settings = ("TRIAGE_OAUTH_ISSUER", "TRIAGE_MCP_RESOURCE_URL", "TRIAGE_OAUTH_PUBLIC_KEY_FILE")
    issuer, resource, key_file = (os.getenv(setting) for setting in settings)
    if not issuer or not resource or not key_file:
        raise ValueError(f"HTTP MCP requires {', '.join(settings)}.")
    try:
        public_key = load_pem_public_key(Path(key_file).read_bytes())
        if not isinstance(public_key, RSAPublicKey) or public_key.key_size < 2048:
            raise ValueError("Invalid RSA public key")
    except (OSError, ValueError, TypeError) as exc:
        raise ValueError("TRIAGE_OAUTH_PUBLIC_KEY_FILE must contain a PEM RSA public key of at least 2048 bits.") from exc
    return create_http_app(source, issuer=issuer, resource=resource, public_key=public_key)
