from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Protocol, Any, Callable, Dict
import ssl


# --- Enum for mechanisms ---
class AuthMechanism(str, Enum):
    NONE = "NONE"
    PLAIN = "PLAIN"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    TLS = "TLS"  # mTLS / client certs


@dataclass
class AuthOptions:
    # Basic SASL
    username: Optional[str] = None
    password: Optional[str] = None

    # TLS/mTLS: either file paths or an already-built ssl.SSLContext
    use_tls: bool = False
    ssl_ca_location: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_key_password: Optional[str] = None
    ssl_context: Optional[ssl.SSLContext] = None

    # Mechanism (redundant, but useful for debugging)
    mechanism: Optional[AuthMechanism] = None


# --- Provider protocol ---
class AuthProvider(Protocol):
    def get_mechanism(self) -> AuthMechanism:
        ...

    def get_auth_options(self) -> AuthOptions:
        ...


# --- Base Provider ---
class BaseAuthProvider:
    def __init__(self, mechanism: AuthMechanism, options: Optional[AuthOptions] = None):
        self._mech = mechanism
        self._opts = options or AuthOptions(mechanism=mechanism)

    def get_mechanism(self) -> AuthMechanism:
        return self._mech

    def get_auth_options(self) -> AuthOptions:
        return self._opts


# --- Factory helpers ---
def new_no_auth() -> AuthProvider:
    return BaseAuthProvider(AuthMechanism.NONE, AuthOptions(mechanism=AuthMechanism.NONE))


def new_plain_auth(username: str, password: str, use_tls: bool = True) -> AuthProvider:
    return BaseAuthProvider(AuthMechanism.PLAIN, AuthOptions(
        username=username,
        password=password,
        use_tls=use_tls,
        mechanism=AuthMechanism.PLAIN,
    ))


def new_scram_auth(
        username: str,
        password: str,
        mech: AuthMechanism = AuthMechanism.SCRAM_SHA_256,
        use_tls: bool = True,
) -> AuthProvider:
    if mech not in (AuthMechanism.SCRAM_SHA_256, AuthMechanism.SCRAM_SHA_512):
        raise ValueError("Invalid SCRAM mechanism")

    return BaseAuthProvider(mech, AuthOptions(
        username=username,
        password=password,
        use_tls=use_tls,
        mechanism=mech,
    ))


def new_tls_auth(
        ca_location: Optional[str] = None,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
        key_password: Optional[str] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
) -> AuthProvider:
    ctx = ssl_context
    if ctx is None:
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if ca_location:
            ctx.load_verify_locations(cafile=ca_location)
        if certfile and keyfile:
            ctx.load_cert_chain(certfile=certfile, keyfile=keyfile, password=key_password)

    return BaseAuthProvider(AuthMechanism.TLS, AuthOptions(
        use_tls=True,
        ssl_ca_location=ca_location,
        ssl_certfile=certfile,
        ssl_keyfile=keyfile,
        ssl_key_password=key_password,
        ssl_context=ctx,
        mechanism=AuthMechanism.TLS,
    ))


# --- Coercion helper ---
def coerce_to_provider(maybe: Any, import_from_string: Callable[[str], Any]) -> AuthProvider:
    """
    Accept:
      - AuthProvider instance
      - dict config
      - import path string
      - None → new_no_auth()
    """
    if maybe is None:
        return new_no_auth()

    if hasattr(maybe, "get_mechanism") and hasattr(maybe, "get_auth_options"):
        return maybe  # Already a provider

    if isinstance(maybe, str):
        obj = import_from_string(maybe)
        return coerce_to_provider(obj, import_from_string)

    if isinstance(maybe, dict):
        mech = maybe.get("sasl_mechanism") or maybe.get("mechanism")

        # Normalize mechanism
        if isinstance(mech, str):
            mech_upper = mech.upper()
            if mech_upper == "PLAIN":
                mechanism = AuthMechanism.PLAIN
            elif "SCRAM" in mech_upper:
                if "512" in mech_upper:
                    mechanism = AuthMechanism.SCRAM_SHA_512
                else:
                    mechanism = AuthMechanism.SCRAM_SHA_256
            elif mech_upper in ("TLS", "MTLS"):
                mechanism = AuthMechanism.TLS
            elif mech_upper in ("AWS_MSK_IAM", "AWSMSKIAM", "AWS_IAM"):
                mechanism = AuthMechanism.AWS_MSK_IAM
            else:
                mechanism = AuthMechanism.NONE
        elif isinstance(mech, AuthMechanism):
            mechanism = mech
        else:
            mechanism = AuthMechanism.NONE

        # Build options from dict
        opts = AuthOptions(
            username=maybe.get("sasl_username") or maybe.get("username"),
            password=maybe.get("sasl_password") or maybe.get("password"),
            use_tls=maybe.get("enable_ssl") or maybe.get("use_tls") or False,
            ssl_ca_location=maybe.get("ssl_ca_location"),
            ssl_certfile=maybe.get("ssl_certificate_location") or maybe.get("ssl_certfile"),
            ssl_keyfile=maybe.get("ssl_key_location") or maybe.get("ssl_keyfile"),
            ssl_key_password=maybe.get("ssl_key_password"),
            mechanism=mechanism,
        )

        # Return provider based on mechanism
        if mechanism == AuthMechanism.PLAIN:
            return new_plain_auth(opts.username or "", opts.password or "", opts.use_tls)

        if mechanism in (AuthMechanism.SCRAM_SHA_256, AuthMechanism.SCRAM_SHA_512):
            return new_scram_auth(opts.username or "", opts.password or "", mechanism, opts.use_tls)

        if mechanism == AuthMechanism.TLS:
            return new_tls_auth(
                opts.ssl_ca_location,
                opts.ssl_certfile,
                opts.ssl_keyfile,
                opts.ssl_key_password,
            )

        return new_no_auth()

    raise TypeError("Unsupported auth specification")

def _add_tls_keys(conf: Dict[str, Any], opts: AuthOptions) -> None:
    """
    Add TLS-related librdkafka keys to the config dict if present in opts.
    Uses file-path style keys that librdkafka expects.
    """
    if opts.ssl_ca_location:
        conf["ssl.ca.location"] = opts.ssl_ca_location
    if opts.ssl_certfile:
        conf["ssl.certificate.location"] = opts.ssl_certfile
    if opts.ssl_keyfile:
        conf["ssl.key.location"] = opts.ssl_keyfile
    if opts.ssl_key_password:
        conf["ssl.key.password"] = opts.ssl_key_password


def _build_tls_only(opts: AuthOptions) -> Dict[str, Any]:
    """
    Build TLS-only config (security.protocol = SSL + ssl.* keys)
    """
    conf: Dict[str, Any] = {"security.protocol": "SSL"}
    _add_tls_keys(conf, opts)
    return conf


def _build_sasl_conf(mech_value: str, opts: AuthOptions) -> Dict[str, Any]:
    """
    Build a SASL conf dict for PLAIN or SCRAM.
    `mech_value` should be a string like "PLAIN" or "SCRAM-SHA-256".
    """
    conf: Dict[str, Any] = {"sasl.mechanisms": mech_value,
                            "security.protocol": "SASL_SSL" if opts.use_tls else "SASL_PLAINTEXT"}
    # prefer encrypted transport when possible

    if opts.username:
        conf["sasl.username"] = opts.username
    if opts.password:
        conf["sasl.password"] = opts.password

    # attach TLS keys (if use_tls)
    if opts.use_tls:
        _add_tls_keys(conf, opts)

    return conf


def build_confluent_auth_config(auth_provider: AuthProvider) -> Dict[str, Any]:
    """
    Convert an AuthProvider to a confluent-kafka / librdkafka configuration dict.

    Returns a dict of librdkafka config keys (e.g. security.protocol, sasl.*,
    ssl.*) that you can `update()` into the final confluent-kafka client config.

    """
    if auth_provider is None:
        return {}

    mech = auth_provider.get_mechanism()
    opts = auth_provider.get_auth_options()

    conf: Dict[str, Any] = {}

    if mech is None or mech is None:
        return conf

    # NONE
    if mech == AuthMechanism.NONE:
        return conf

    # PLAIN
    if mech == AuthMechanism.PLAIN:
        if not (opts.username and opts.password):
            raise ValueError("PLAIN auth requires username and password")
        conf.update(_build_sasl_conf("PLAIN", opts))
        return conf

    # SCRAM-SHA-256 / SCRAM-SHA-512
    if mech in (AuthMechanism.SCRAM_SHA_256, AuthMechanism.SCRAM_SHA_512):
        if not (opts.username and opts.password):
            raise ValueError(f"{mech.value()} requires username and password")
        conf.update(_build_sasl_conf(mech.value(), opts))
        return conf

    # TLS-only (mTLS)
    if mech == AuthMechanism.TLS:
        # for TLS mechanism we require at least cert/key or CA, or an indication
        if not (opts.ssl_ca_location or opts.ssl_certfile or opts.ssl_keyfile):
            raise ValueError("TLS auth requires ssl_ca_location or client cert/key paths in AuthOptions")
        conf.update(_build_tls_only(opts))
        return conf


    # fallback (shouldn't happen)
    raise conf
