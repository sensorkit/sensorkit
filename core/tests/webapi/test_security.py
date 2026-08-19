# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import datetime
import ipaddress
import ssl

import httpx
import pytest
import pytest_asyncio
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import TypeAdapter, ValidationError

from sensorkit.webapi.fastapi import WebAPI, WebAPIConfig
from sensorkit.webapi.security import (
    DOC_PATHS,
    AuthConfig,
    CORSConfig,
    NoAuthConfig,
    SecurityConfigError,
    TLSConfig,
    TokenAuthConfig,
)

TOKEN = "a-token-long-enough-to-not-warn"


async def null_app(scope, receive, send):
    """Stand-in application, since loading a uvicorn config also resolves the app."""


def write_certificate(directory, *, password: bytes | None = None):
    """Write a throwaway self-signed certificate and key, and return their paths."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.UTC)

    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    encryption = (
        serialization.BestAvailableEncryption(password)
        if password
        else serialization.NoEncryption()
    )

    certfile = directory / "cert.pem"
    keyfile = directory / "key.pem"

    certfile.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    keyfile.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            encryption,
        )
    )

    return certfile, keyfile


@pytest_asyncio.fixture
async def webapi_client(kit):
    """Return a factory building a WebAPI and an httpx client against its app."""
    built = []

    async with asyncio.TaskGroup() as tg:

        async def build(**kwargs):
            webapi = WebAPI(kit, WebAPIConfig(**kwargs))

            await webapi.kv_forwarder.start(task_group=tg)
            await webapi.stream_forwarder.start(task_group=tg)

            transport = httpx.ASGITransport(app=webapi.app)
            client = httpx.AsyncClient(transport=transport, base_url="http://test")

            built.append((webapi, client))
            return webapi, client

        yield build

        for webapi, client in built:
            await client.aclose()
            await webapi.shutdown()


# ---------------------------------------------------------------------------
# TLS configuration
# ---------------------------------------------------------------------------

def test_client_cert_without_ca_is_rejected(tmp_path):
    certfile, keyfile = write_certificate(tmp_path)

    with pytest.raises(ValidationError):
        TLSConfig(certfile=certfile, keyfile=keyfile, require_client_cert=True)


def test_uvicorn_options_default_to_no_client_verification(tmp_path):
    certfile, keyfile = write_certificate(tmp_path)
    options = TLSConfig(certfile=certfile, keyfile=keyfile).uvicorn_options()

    assert options["ssl_cert_reqs"] == ssl.CERT_NONE
    assert options["ssl_ca_certs"] is None
    assert options["ssl_keyfile_password"] is None


def test_uvicorn_options_require_client_certificate(tmp_path):
    certfile, keyfile = write_certificate(tmp_path)
    config = TLSConfig(
        certfile=certfile,
        keyfile=keyfile,
        ca_certs=certfile,
        require_client_cert=True,
    )

    assert config.uvicorn_options()["ssl_cert_reqs"] == ssl.CERT_REQUIRED


def test_missing_key_password_env_fails(tmp_path, monkeypatch):
    certfile, keyfile = write_certificate(tmp_path, password=b"hunter2")
    monkeypatch.delenv("SK_TEST_KEY_PASSWORD", raising=False)
    config = TLSConfig(
        certfile=certfile, keyfile=keyfile, keyfile_password_env="SK_TEST_KEY_PASSWORD"
    )

    with pytest.raises(SecurityConfigError):
        config.uvicorn_options()


def test_encrypted_key_loads_with_password_from_env(tmp_path, monkeypatch):
    certfile, keyfile = write_certificate(tmp_path, password=b"hunter2")
    monkeypatch.setenv("SK_TEST_KEY_PASSWORD", "hunter2")
    config = TLSConfig(
        certfile=certfile, keyfile=keyfile, keyfile_password_env="SK_TEST_KEY_PASSWORD"
    )

    # Loading is what builds the SSL context, so a wrong password fails here.
    uvicorn_config = uvicorn.Config(app=null_app, **config.uvicorn_options())
    uvicorn_config.load()

    assert uvicorn_config.ssl is not None


def test_minimum_version_raises_the_protocol_floor(tmp_path):
    certfile, keyfile = write_certificate(tmp_path)
    config = TLSConfig(certfile=certfile, keyfile=keyfile, minimum_version="1.3")

    uvicorn_config = uvicorn.Config(app=null_app, **config.uvicorn_options())
    uvicorn_config.load()
    config.apply_minimum_version(uvicorn_config.ssl)

    assert uvicorn_config.ssl.minimum_version == ssl.TLSVersion.TLSv1_3


def test_bad_certificate_fails_when_the_server_starts(tmp_path):
    certfile, keyfile = write_certificate(tmp_path)
    certfile.write_text("not a certificate")
    config = TLSConfig(certfile=certfile, keyfile=keyfile)

    uvicorn_config = uvicorn.Config(app=null_app, **config.uvicorn_options())

    with pytest.raises(ssl.SSLError):
        uvicorn_config.load()


# ---------------------------------------------------------------------------
# Token configuration
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kwargs", [
    {},
    {"token_env": "SK_TEST_TOKEN", "token_file": "token.txt"},
])
def test_token_auth_needs_exactly_one_source(kwargs):
    with pytest.raises(ValidationError):
        TokenAuthConfig(**kwargs)


def test_unset_token_env_fails(monkeypatch):
    monkeypatch.delenv("SK_TEST_TOKEN", raising=False)

    with pytest.raises(SecurityConfigError):
        TokenAuthConfig(token_env="SK_TEST_TOKEN").create_authenticator()


def test_missing_token_file_fails(tmp_path):
    with pytest.raises(SecurityConfigError):
        TokenAuthConfig(token_file=tmp_path / "absent.txt").create_authenticator()


def test_empty_token_file_fails(tmp_path):
    path = tmp_path / "token.txt"
    path.write_text("   \n")

    with pytest.raises(SecurityConfigError):
        TokenAuthConfig(token_file=path).create_authenticator()


def test_token_file_is_stripped(tmp_path):
    path = tmp_path / "token.txt"
    path.write_text(f"  {TOKEN}\n")
    auth = TokenAuthConfig(token_file=path).create_authenticator()

    assert auth.authorize("POST", "/agent/enable", f"Bearer {TOKEN}")


@pytest.mark.parametrize("authorization", [
    None,
    "",
    "Bearer",
    "Bearer wrong",
    f"Basic {TOKEN}",
    TOKEN,
])
def test_bad_credentials_are_refused(monkeypatch, authorization):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    auth = TokenAuthConfig(token_env="SK_TEST_TOKEN").create_authenticator()

    assert not auth.authorize("POST", "/agent/enable", authorization)


def test_anonymous_read_covers_only_safe_methods(monkeypatch):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    auth = TokenAuthConfig(
        token_env="SK_TEST_TOKEN", allow_anonymous_read=True
    ).create_authenticator()

    assert auth.authorize("GET", "/data/snapshot", None)
    assert not auth.authorize("POST", "/agent/enable", None)


@pytest.mark.parametrize("path", sorted(DOC_PATHS))
def test_anonymous_read_excludes_the_schema(monkeypatch, path):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    auth = TokenAuthConfig(
        token_env="SK_TEST_TOKEN", allow_anonymous_read=True
    ).create_authenticator()

    assert not auth.authorize("GET", path, None)
    assert auth.authorize("GET", path, f"Bearer {TOKEN}")


def test_auth_config_discriminates_on_kind():
    adapter = TypeAdapter(AuthConfig)

    assert isinstance(adapter.validate_python({"kind": "none"}), NoAuthConfig)
    assert isinstance(
        adapter.validate_python({"kind": "token", "token_env": "X"}), TokenAuthConfig
    )


# ---------------------------------------------------------------------------
# CORS configuration
# ---------------------------------------------------------------------------

def test_cors_defaults_to_no_origins():
    assert CORSConfig().allow_origins == []


def test_credentialed_wildcard_origin_is_rejected():
    with pytest.raises(ValidationError):
        CORSConfig(allow_origins=["*"], allow_credentials=True)


# ---------------------------------------------------------------------------
# Enforcement through the app
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_api_serves_without_credentials(webapi_client):
    _, client = await webapi_client()

    assert (await client.get("/data/snapshot")).status_code == 200


@pytest.mark.asyncio
async def test_token_auth_refuses_unauthenticated_requests(webapi_client, monkeypatch):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    _, client = await webapi_client(auth={"kind": "token", "token_env": "SK_TEST_TOKEN"})

    resp = await client.get("/data/snapshot")
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"

    resp = await client.get("/data/snapshot", headers={"Authorization": f"Bearer {TOKEN}"})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_token_auth_guards_mutating_endpoints(webapi_client, monkeypatch):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    _, client = await webapi_client(
        auth={"kind": "token", "token_env": "SK_TEST_TOKEN", "allow_anonymous_read": True}
    )

    assert (await client.get("/data/snapshot")).status_code == 200

    resp = await client.post("/device/mydevice/command", json={"command_id": "Abort"})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_openapi_advertises_the_bearer_scheme(webapi_client, monkeypatch):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    webapi, _ = await webapi_client(auth={"kind": "token", "token_env": "SK_TEST_TOKEN"})

    schema = webapi.app.openapi_schema
    assert schema["components"]["securitySchemes"]["bearerAuth"]["scheme"] == "bearer"
    assert schema["security"] == [{"bearerAuth": []}]


@pytest.mark.asyncio
async def test_no_cross_origin_access_by_default(webapi_client):
    _, client = await webapi_client()

    resp = await client.get("/data/snapshot", headers={"Origin": "http://elsewhere"})
    assert "access-control-allow-origin" not in resp.headers


@pytest.mark.asyncio
async def test_configured_origin_is_allowed(webapi_client):
    _, client = await webapi_client(cors={"allow_origins": ["http://dashboard"]})

    resp = await client.get("/data/snapshot", headers={"Origin": "http://dashboard"})
    assert resp.headers["access-control-allow-origin"] == "http://dashboard"


@pytest.mark.asyncio
async def test_preflight_is_answered_without_credentials(webapi_client, monkeypatch):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    _, client = await webapi_client(
        auth={"kind": "token", "token_env": "SK_TEST_TOKEN"},
        cors={"allow_origins": ["http://dashboard"]},
    )

    # A browser sends the preflight without the Authorization header it is asking
    # permission to use, so rejecting it would deny every cross-origin call.
    resp = await client.options(
        "/device/mydevice/command",
        headers={
            "Origin": "http://dashboard",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization",
        },
    )

    assert resp.status_code == 200
    assert resp.headers["access-control-allow-origin"] == "http://dashboard"


@pytest.mark.asyncio
async def test_security_headers_are_set(webapi_client):
    _, client = await webapi_client()

    resp = await client.get("/data/snapshot")
    assert resp.headers["x-content-type-options"] == "nosniff"
    # No TLS, so promising HSTS would strand clients on a scheme that does not answer.
    assert "strict-transport-security" not in resp.headers


@pytest.mark.asyncio
async def test_hsts_is_set_when_tls_is_configured(webapi_client, tmp_path):
    certfile, keyfile = write_certificate(tmp_path)
    _, client = await webapi_client(tls={"certfile": str(certfile), "keyfile": str(keyfile)})

    resp = await client.get("/data/snapshot")
    assert resp.headers["strict-transport-security"] == "max-age=31536000"


@pytest.mark.asyncio
async def test_docs_are_withheld_by_default(webapi_client):
    _, client = await webapi_client()

    assert (await client.get("/docs")).status_code == 404
    assert (await client.get("/redoc")).status_code == 404
    # The schema still serves, so a client can generate against an open deployment.
    assert (await client.get("/openapi.json")).status_code == 200


@pytest.mark.asyncio
async def test_docs_can_be_exposed(webapi_client):
    _, client = await webapi_client(expose_docs=True)

    assert (await client.get("/docs")).status_code == 200


@pytest.mark.asyncio
async def test_anonymous_readers_cannot_fetch_the_schema(webapi_client, monkeypatch):
    monkeypatch.setenv("SK_TEST_TOKEN", TOKEN)
    _, client = await webapi_client(
        expose_docs=True,
        auth={"kind": "token", "token_env": "SK_TEST_TOKEN", "allow_anonymous_read": True},
    )

    assert (await client.get("/openapi.json")).status_code == 401
    assert (await client.get("/docs")).status_code == 401

    resp = await client.get("/openapi.json", headers={"Authorization": f"Bearer {TOKEN}"})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_stream_subscribers_are_capped(webapi_client):
    webapi, client = await webapi_client(max_stream_clients=1)

    # Occupy the single slot the way a live subscriber does.
    webapi.client_queues.add(asyncio.Queue())

    resp = await client.get("/data/subscribe")
    assert resp.status_code == 503
