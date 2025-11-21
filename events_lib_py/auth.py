from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any, Callable, Dict
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



# --- Coercion helper ---
def coerce_to_provider(maybe: Any, import_from_string: Callable[[str], Any]) -> AuthOptions:
    """
    Accept:
      - AuthOptions instance
      - dict config
      - import path string (use import_from_string to load)
      - provider-like object with attributes
      - None -> new no-auth AuthOptions (mechanism=NONE)

    Returns:
      AuthOptions
    """
    # 1) Already an AuthOptions
    if isinstance(maybe, AuthOptions):
        return maybe

    # 2) None -> no auth
    if maybe is None:
        return AuthOptions(mechanism=AuthMechanism.NONE)

    # 3) If a string -> import it and re-coerce the result
    if isinstance(maybe, str):
        loaded = import_from_string(maybe)
        # recursively coerce the loaded object (avoid infinite recursion by ensuring loaded != same string)
        return coerce_to_provider(loaded, import_from_string)

    # 4) If dict -> map keys to AuthOptions fields
    if isinstance(maybe, dict):
        d = maybe.copy()
        mech = d.get("mechanism")
        if mech is not None:
            # Allow either AuthMechanism or string
            if isinstance(mech, AuthMechanism):
                mech_enum = mech
            else:
                # accept values like "plain", "PLAIN", "SCRAM-SHA-256", "tls", etc.
                try:
                    mech_enum = AuthMechanism(mech)
                except Exception:
                    # try uppercase fallback for simple strings like "plain" or "tls"
                    try:
                        mech_enum = AuthMechanism(str(mech).upper())
                    except Exception:
                        raise ValueError(f"Unknown mechanism value: {mech!r}")
        else:
            mech_enum = None

        return AuthOptions(
            username=d.get("username"),
            password=d.get("password"),
            use_tls=bool(d.get("use_tls", False)),
            ssl_ca_location=d.get("ssl_ca_location") or d.get("ssl.ca.location"),
            ssl_certfile=d.get("ssl_certfile") or d.get("ssl.certificate.location"),
            ssl_keyfile=d.get("ssl_keyfile") or d.get("ssl.key.location"),
            ssl_key_password=d.get("ssl_key_password") or d.get("ssl.key.password"),
            ssl_context=d.get("ssl_context"),
            mechanism=mech_enum,
        )

    # 5) If it's an object (e.g. provider instance) try to extract expected attributes
    # This supports e.g. an AuthProvider instance that exposes similar-named attrs or a method returning options
    if hasattr(maybe, "__dict__") or not isinstance(maybe, (int, float, bool, bytes, bytearray)):
        # If provider exposes a method to get options, prefer that (common names)
        for getter_name in ("to_auth_options", "get_auth_options", "auth_options", "as_auth_options"):
            getter = getattr(maybe, getter_name, None)
            if callable(getter):
                result = getter()
                # If method returns dict or AuthOptions, coerce recursively
                return coerce_to_provider(result, import_from_string)

        # Fall back to reading attributes by name
        username = getattr(maybe, "username", None)
        password = getattr(maybe, "password", None)
        use_tls = getattr(maybe, "use_tls", getattr(maybe, "useTls", False))
        ssl_ca_location = getattr(maybe, "ssl_ca_location", getattr(maybe, "ssl_ca", None))
        ssl_certfile = getattr(maybe, "ssl_certfile", getattr(maybe, "ssl_certfile_path", None))
        ssl_keyfile = getattr(maybe, "ssl_keyfile", getattr(maybe, "ssl_keyfile_path", None))
        ssl_key_password = getattr(maybe, "ssl_key_password", None)
        ssl_context = getattr(maybe, "ssl_context", None)
        mech = getattr(maybe, "mechanism", None)
        if mech is not None and not isinstance(mech, AuthMechanism):
            try:
                mech = AuthMechanism(mech)
            except Exception:
                try:
                    mech = AuthMechanism(str(mech).upper())
                except Exception:
                    raise ValueError(f"Unknown mechanism value on provider object: {mech!r}")

        # If any of the above attributes exist, return an AuthOptions built from them
        if any([username, password, use_tls, ssl_ca_location, ssl_certfile, ssl_keyfile, ssl_context, mech]):
            return AuthOptions(
                username=username,
                password=password,
                use_tls=bool(use_tls),
                ssl_ca_location=ssl_ca_location,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                ssl_key_password=ssl_key_password,
                ssl_context=ssl_context,
                mechanism=mech,
            )

    # Not something we can coerce
    raise TypeError("Unsupported auth provider type; expected AuthOptions, dict, import path string, provider object, or None")

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


def build_confluent_auth_config(opts: AuthOptions) -> Dict[str, Any]:
    """
    Convert an AuthProvider to a confluent-kafka / librdkafka configuration dict.

    Returns a dict of librdkafka config keys (e.g. security.protocol, sasl.*,
    ssl.*) that you can `update()` into the final confluent-kafka client config.

    """
    if opts is None:
        return {}

    mech = opts.mechanism
    conf: Dict[str, Any] = {}

    if mech is None:
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
            raise ValueError(f"{mech.value} requires username and password")
        conf.update(_build_sasl_conf(mech.value, opts))
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
