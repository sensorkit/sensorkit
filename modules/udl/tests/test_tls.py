# SPDX-License-Identifier: Apache-2.0
import asyncio
import ipaddress
import ssl
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from sensorkit.udl.models import UDLEndpointConfig


def issue(name, issuer=None, *, ca=False, usage=None, ip=None):
    """Issue a certificate signed by `issuer` (a cert/key pair), or self-signed."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    issuer_cert, issuer_key = issuer or (None, key)
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer_cert.subject if issuer_cert else subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_key.public_key()),
            critical=False,
        )
    )

    if ca:
        builder = builder.add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )

    if usage:
        builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)

    if ip:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(ip))]), critical=False
        )

    return builder.sign(issuer_key, hashes.SHA256()), key


def write_pem(path, cert, key=None):
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    if key is None:
        return path

    key_path = path.with_suffix(".key")
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return path, key_path


@pytest.fixture
def pki(tmp_path):
    """A private CA issuing a server leaf and a client cert, as in a mutual-TLS deployment."""
    ca = issue("test-ca", ca=True)
    server = issue("server", ca, usage=ExtendedKeyUsageOID.SERVER_AUTH, ip="127.0.0.1")
    client = issue("client", ca, usage=ExtendedKeyUsageOID.CLIENT_AUTH)
    other = issue("other", ca=True)

    return {
        "ca": write_pem(tmp_path / "ca.pem", ca[0]),
        "server": write_pem(tmp_path / "server.pem", *server),
        "server_leaf": write_pem(tmp_path / "server-leaf.pem", server[0]),
        "client": write_pem(tmp_path / "client.pem", *client),
        "other": write_pem(tmp_path / "other.pem", other[0]),
    }


@pytest_asyncio.fixture
async def mtls_server(pki):
    """An HTTPS server that requires a client certificate issued by the test CA."""
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=str(pki["ca"]))
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(*map(str, pki["server"]))

    async def respond(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            await writer.drain()
        except (ssl.SSLError, ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(respond, "127.0.0.1", 0, ssl=context)
    port = server.sockets[0].getsockname()[1]

    async with server:
        yield f"https://127.0.0.1:{port}/"


def endpoint(pki, *, verify, client=True):
    cert, key = map(str, pki["client"]) if client else (None, None)
    return UDLEndpointConfig(client_cert=cert, client_key=key, client_verify=verify)


async def get(url, config):
    async with httpx.AsyncClient(timeout=10, **config.tls_options()) as http:
        return await http.get(url)


# --- Pinned trust anchor ---


@pytest.mark.asyncio
async def test_pinned_server_cert_verifies_and_presents_client_cert(pki, mtls_server):
    response = await get(mtls_server, endpoint(pki, verify=str(pki["server_leaf"])))

    assert response.text == "ok"


@pytest.mark.asyncio
async def test_private_ca_bundle_verifies(pki, mtls_server):
    response = await get(mtls_server, endpoint(pki, verify=str(pki["ca"])))

    assert response.text == "ok"


@pytest.mark.asyncio
async def test_pinned_path_without_client_cert_is_rejected(pki, mtls_server):
    with pytest.raises(httpx.HTTPError):
        await get(mtls_server, endpoint(pki, verify=str(pki["server_leaf"]), client=False))


@pytest.mark.asyncio
async def test_untrusted_server_cert_fails_verification(pki, mtls_server):
    with pytest.raises(httpx.ConnectError):
        await get(mtls_server, endpoint(pki, verify=str(pki["other"])))


@pytest.mark.asyncio
async def test_system_store_rejects_private_ca(pki, mtls_server):
    with pytest.raises(httpx.ConnectError):
        await get(mtls_server, endpoint(pki, verify=True))
