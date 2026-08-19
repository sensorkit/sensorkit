# SPDX-License-Identifier: Apache-2.0
import os
import pathlib
import secrets
import ssl
from abc import ABC, abstractmethod
from typing import Annotated, Any, Literal, override

from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field, model_validator

BEARER_PREFIX = "Bearer "
READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
DOC_PATHS = frozenset({"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"})
SHORT_TOKEN_LENGTH = 16

TLS_VERSIONS = {
    "1.2": ssl.TLSVersion.TLSv1_2,
    "1.3": ssl.TLSVersion.TLSv1_3,
}


class SecurityConfigError(Exception):
    """The security configuration cannot be applied on this host."""


class TLSConfig(BaseModel):
    """TLS settings for the web API listener.

    Key material stays on disk. This record is written to the key-value store, which
    every service on the bus can read, so it carries file paths and the name of the
    environment variable holding the key password rather than the secrets themselves.
    """

    certfile: pathlib.Path
    keyfile: pathlib.Path
    keyfile_password_env: str | None = None
    ca_certs: pathlib.Path | None = None
    require_client_cert: bool = False
    minimum_version: Literal["1.2", "1.3"] = "1.2"
    ciphers: str | None = None

    @model_validator(mode="after")
    def _check_verify_source(self):
        if self.require_client_cert and self.ca_certs is None:
            raise ValueError("require_client_cert needs 'ca_certs' to verify clients against")

        return self

    def uvicorn_options(self) -> dict[str, Any]:
        """Return the ssl keyword arguments for a uvicorn config.

        Raises:
            SecurityConfigError: The named password variable is unset or empty.
        """
        password = None

        if self.keyfile_password_env:
            password = os.environ.get(self.keyfile_password_env)

            if not password:
                raise SecurityConfigError(
                    f"env var {self.keyfile_password_env!r} holds no private key password"
                )

        options = {
            "ssl_certfile": self.certfile,
            "ssl_keyfile": self.keyfile,
            "ssl_keyfile_password": password,
            "ssl_ca_certs": str(self.ca_certs) if self.ca_certs else None,
            "ssl_cert_reqs": ssl.CERT_REQUIRED if self.require_client_cert else ssl.CERT_NONE,
        }

        if self.ciphers:
            options["ssl_ciphers"] = self.ciphers

        return options

    def apply_minimum_version(self, context: ssl.SSLContext):
        """Raise the floor on the protocol versions the listener will negotiate.

        The server builds its own context from the uvicorn options, and the version
        floor is not one of them, so it is set on the finished context.
        """
        context.minimum_version = TLS_VERSIONS[self.minimum_version]


class Authenticator(ABC):
    """Decides whether a request carries acceptable credentials."""

    @property
    @abstractmethod
    def enabled(self) -> bool:
        """Whether any credential is required at all."""

    @abstractmethod
    def authorize(self, method: str, path: str, authorization: str | None) -> bool:
        """Return whether a request for `path` using `method` and `authorization` may proceed."""

    def openapi_scheme(self) -> dict[str, Any] | None:
        """Return the OpenAPI security scheme to advertise, where one applies."""
        return None


class OpenAccess(Authenticator):
    """Accepts every request."""

    @property
    @override
    def enabled(self):
        return False

    @override
    def authorize(self, method: str, path: str, authorization: str | None):
        return True


class BearerToken(Authenticator):
    """Accepts requests presenting a shared bearer token."""

    def __init__(self, token: str, *, allow_anonymous_read: bool):
        self.token = token
        self.allow_anonymous_read = allow_anonymous_read

    @property
    @override
    def enabled(self):
        return True

    @override
    def authorize(self, method: str, path: str, authorization: str | None):
        # The schema and the documentation UIs map out the endpoints an anonymous reader
        # is not allowed to reach, so they stay behind the token.
        if self.allow_anonymous_read and method in READ_METHODS and path not in DOC_PATHS:
            return True

        if authorization is None or not authorization.startswith(BEARER_PREFIX):
            return False

        return secrets.compare_digest(authorization[len(BEARER_PREFIX) :], self.token)

    @override
    def openapi_scheme(self):
        return {"type": "http", "scheme": "bearer"}


class NoAuthConfig(BaseModel):
    """Leave the web API open to anyone who can reach the port."""

    kind: Literal["none"] = "none"

    def create_authenticator(self) -> Authenticator:
        return OpenAccess()


class TokenAuthConfig(BaseModel):
    """Require a shared bearer token on requests to the web API.

    The token itself is never part of this record, since the key-value store holding
    it is readable across the bus. Name an environment variable or a file instead.
    """

    kind: Literal["token"] = "token"
    token_env: str | None = None
    token_file: pathlib.Path | None = None
    allow_anonymous_read: bool = False

    @model_validator(mode="after")
    def _check_one_source(self):
        if (self.token_env is None) == (self.token_file is None):
            raise ValueError("token auth needs exactly one of 'token_env' or 'token_file'")

        return self

    def create_authenticator(self) -> Authenticator:
        """Build the authenticator, reading the token from its configured source.

        Raises:
            SecurityConfigError: The source is unreadable or holds nothing.
        """
        if self.token_env is not None:
            token = os.environ.get(self.token_env, "")
            source = f"env var {self.token_env!r}"
        else:
            assert self.token_file is not None

            try:
                token = self.token_file.read_text(encoding="utf-8").strip()
            except OSError as err:
                raise SecurityConfigError(
                    f"cannot read web API token file {self.token_file}"
                ) from err

            source = f"token file {self.token_file}"

        if not token:
            raise SecurityConfigError(f"web API {source} holds no token")

        if len(token) < SHORT_TOKEN_LENGTH:
            logger.warning(
                f"web API token from {source} is shorter than {SHORT_TOKEN_LENGTH} characters "
                f"and is weak against guessing"
            )

        return BearerToken(token, allow_anonymous_read=self.allow_anonymous_read)


type AuthConfig = Annotated[NoAuthConfig | TokenAuthConfig, Field(discriminator="kind")]


class CORSConfig(BaseModel):
    """Cross-origin rules for browser clients.

    No origin is permitted by default. A dashboard served from somewhere other than
    the web API's own origin needs listing here.
    """

    allow_origins: list[str] = []
    allow_origin_regex: str | None = None
    allow_credentials: bool = False
    allow_methods: list[str] = ["*"]
    allow_headers: list[str] = ["*"]

    @model_validator(mode="after")
    def _check_credentialed_wildcard(self):
        # Browsers reject a wildcard origin on a credentialed response, so the
        # combination would silently deny every request it appears to allow.
        if self.allow_credentials and "*" in self.allow_origins:
            raise ValueError("allow_credentials cannot be used with a wildcard origin")

        return self

    def middleware_options(self) -> dict[str, Any]:
        """Return the keyword arguments for the CORS middleware."""
        return {
            "allow_origins": self.allow_origins,
            "allow_origin_regex": self.allow_origin_regex,
            "allow_credentials": self.allow_credentials,
            "allow_methods": self.allow_methods,
            "allow_headers": self.allow_headers,
        }


class AuthMiddleware:
    """Rejects requests the authenticator does not permit, before routing.

    Enforcing here rather than per route means an endpoint added later is covered
    without having to remember a dependency.
    """

    def __init__(self, app, authenticator: Authenticator):
        self.app = app
        self.authenticator = authenticator

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        authorization = next(
            (value.decode("latin-1") for key, value in scope["headers"] if key == b"authorization"),
            None,
        )

        if not self.authenticator.authorize(scope["method"], scope["path"], authorization):
            response = JSONResponse(
                {"detail": "Not authenticated"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


class SecurityHeadersMiddleware:
    """Adds response headers that constrain how browsers treat the API."""

    def __init__(self, app, *, hsts: bool):
        self.app = app
        self.hsts = hsts

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        extra = [(b"x-content-type-options", b"nosniff")]

        if self.hsts:
            extra.append((b"strict-transport-security", b"max-age=31536000"))

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), *extra]

            await send(message)

        await self.app(scope, receive, send_with_headers)
