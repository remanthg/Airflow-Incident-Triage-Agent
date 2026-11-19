import asyncio
import socket
import time
from uuid import uuid4

import httpx
import jwt
import uvicorn
from cryptography.hazmat.primitives.asymmetric import rsa

from triage.cli import DEMO_ATTEMPTS
from triage.http_auth import LOG_READ_SCOPE, create_http_app
from triage.log_client import fetch_task_log


def test_oauth_protected_mcp_over_loopback_http(monkeypatch):
    async def check():
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        issuer = "https://issuer.example/"
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            resource = f"http://127.0.0.1:{listener.getsockname()[1]}/mcp"
            app = create_http_app("fixtures", issuer=issuer, resource=resource, public_key=key.public_key())
            server = uvicorn.Server(uvicorn.Config(app, log_level="critical", access_log=False))
            running = asyncio.create_task(server.serve(sockets=[listener]))

            def issue(*, signing_key=key, typ="at+jwt", **overrides):
                now = int(time.time())
                payload = {"iss": issuer, "aud": resource, "exp": now + 60, "iat": now,
                           "sub": "test-operator", "client_id": "test-client", "jti": uuid4().hex,
                           "scope": LOG_READ_SCOPE, **overrides}
                return jwt.encode(payload, signing_key, algorithm="RS256", headers={"typ": typ})

            try:
                async with asyncio.timeout(5):
                    while not server.started:
                        if running.done():
                            await running
                            raise RuntimeError("Loopback MCP server stopped during startup.")
                        await asyncio.sleep(0.01)
                attempt = DEMO_ATTEMPTS[0]
                request = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                           "params": {"name": "get_task_log", "arguments": attempt.model_dump()}}
                rejected = [
                    (None, 401, "invalid_token"),
                    ("invalid", 401, "invalid_token"),
                    (issue(signing_key=other_key), 401, "invalid_token"),
                    (issue(exp=int(time.time()) - 1), 401, "invalid_token"),
                    (issue(aud="https://different-resource.example/mcp"), 401, "invalid_token"),
                    (issue(iss="https://different-issuer.example/"), 401, "invalid_token"),
                    (issue(typ="JWT"), 401, "invalid_token"),
                    (issue(scope="logs:write"), 403, "insufficient_scope"),
                ]
                metadata_url = resource.removesuffix("/mcp") + "/.well-known/oauth-protected-resource/mcp"
                async with httpx.AsyncClient(trust_env=False) as client:
                    for token, status, error in rejected:
                        headers = {"Accept": "application/json, text/event-stream"}
                        if token:
                            headers["Authorization"] = f"Bearer {token}"
                        response = await client.post(resource, headers=headers, json=request)
                        assert response.status_code == status
                        assert response.json()["error"] == error
                        challenge = response.headers["www-authenticate"]
                        assert f'resource_metadata="{metadata_url}"' in challenge
                        if status == 403:
                            assert f'scope="{LOG_READ_SCOPE}"' in challenge
                    metadata = (await client.get(metadata_url)).json()
                    assert metadata["resource"] == resource
                    assert metadata["authorization_servers"] == [issuer]
                    assert metadata["scopes_supported"] == [LOG_READ_SCOPE]

                monkeypatch.setenv("TRIAGE_MCP_ACCESS_TOKEN", issue())
                log = await fetch_task_log(attempt, mcp_url=resource)
                assert log.source == "triage/data/fixtures/logs/dev-01.log"
                assert "FileNotFoundError" in log.lines[3].text
            finally:
                server.should_exit = True
                await asyncio.wait_for(running, timeout=5)

    asyncio.run(check())
