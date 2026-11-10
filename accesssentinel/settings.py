"""Runtime settings for deployed services, read from environment variables (and `.env` if present).

Separation of concerns:
  * `config.py`   - research hyper-parameters ([D3] / [IMPL] tagged), versioned with the code.
  * `settings.py` - deployment wiring: hosts, ports, credentials. Never committed with real values.

Every variable is prefixed `AS_` (e.g. `AS_KAFKA_BOOTSTRAP`). Secrets have no defaults: a service that
needs one fails at start-up with a clear error instead of silently using a hard-coded password.
"""
from functools import lru_cache
from typing import Optional
from urllib.parse import quote

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AS_", env_file=".env", env_file_encoding="utf-8",
                                      extra="ignore", env_ignore_empty=True)   # VAR= means "use the default"

    environment: str = "dev"                 # dev | test | prod
    log_level: str = "INFO"

    # --- Kafka -------------------------------------------------------------------------------------
    kafka_bootstrap: str = "localhost:9092"  # host default; containers override with kafka:29092
    kafka_topic_prefix: str = ""             # "" in deployment; tests/checks use a unique prefix
    kafka_partitions: int = 8
    kafka_replication: int = 1               # single-broker dev cluster

    # --- HDFS (WebHDFS) ----------------------------------------------------------------------------
    hdfs_url: str = "http://localhost:9870"
    hdfs_user: str = "root"
    hdfs_base: str = "/accesssentinel"

    # --- PostgreSQL --------------------------------------------------------------------------------
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "accesssentinel"
    postgres_user: str = "accesssentinel"
    postgres_password: Optional[SecretStr] = None

    # --- Redis -------------------------------------------------------------------------------------
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[SecretStr] = None

    # --- Gateway and protected applications --------------------------------------------------------
    upstream_token: Optional[SecretStr] = None      # gateway -> demo apps; apps reject requests without it
    internal_token: Optional[SecretStr] = None      # service -> gateway internal API (session revocation)
    cookie_secure: bool = False                     # True when served over HTTPS
    session_idle_seconds: int = 1800
    session_max_seconds: int = 8 * 3600
    login_max_failures: int = 5                     # per user, within login_lock_seconds
    login_lock_seconds: int = 900
    max_body_bytes: int = 1_000_000
    catalog_refresh_seconds: float = 30.0
    spool_dir: str = "accesssentinel_data/spool"    # local fallback when Kafka cannot accept an event
    demo_user_password: Optional[SecretStr] = None  # seed only; refused when environment=prod

    # --- Stream processing and history (Phase 4) ---------------------------------------------------
    history_backend: str = "hdfs"                   # hdfs | local (local = development-only fallback)
    history_local_dir: str = "accesssentinel_data/history"
    consumer_batch: int = 500
    sink_flush_rows: int = 2000                     # HDFS sink: write a file at this many buffered rows ...
    sink_flush_seconds: float = 30.0                # ... or when the oldest buffered row is this old
    sink_compact_max_files: int = 24                # compact a partition once it has more part files
    dedup_ttl_seconds: int = 86400                  # how long written ids are remembered for redelivery dedup
    spool_replay_seconds: float = 60.0              # gateway: how often to replay spooled events

    # --- TCN individual detection (Phase 5) -----------------------------------------------------------
    model_dir: str = "models"                       # registry root; workers serve <model_dir>/tcn/CURRENT
    tcn_recheck_seconds: Optional[float] = None     # None -> RiskConfig.t_recheck (600 s, D3); override for tests
    tcn_live_window_seconds: float = 3600.0         # events older than this are history/replay: no rechecks
    tcn_model_poll_seconds: float = 30.0            # how often workers look for a newly promoted model
    torch_threads: int = 2

    @field_validator("history_backend")
    @classmethod
    def _backend(cls, v: str) -> str:
        if v not in {"hdfs", "local"}:
            raise ValueError("AS_HISTORY_BACKEND must be hdfs or local")
        return v

    @field_validator("environment")
    @classmethod
    def _env(cls, v: str) -> str:
        if v not in {"dev", "test", "prod"}:
            raise ValueError("AS_ENVIRONMENT must be dev, test or prod")
        return v

    @field_validator("kafka_partitions", "kafka_replication")
    @classmethod
    def _positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("must be >= 1")
        return v

    @staticmethod
    def _require(secret: Optional[SecretStr], name: str) -> str:
        if secret is None or not secret.get_secret_value():
            raise RuntimeError(f"{name} is not set. Copy .env.example to .env and set it.")
        return secret.get_secret_value()

    def postgres_dsn(self) -> str:
        """libpq connection string. Contains the password: never log it."""
        pw = quote(self._require(self.postgres_password, "AS_POSTGRES_PASSWORD"), safe="")
        return (f"postgresql://{quote(self.postgres_user, safe='')}:{pw}@{self.postgres_host}:"
                f"{self.postgres_port}/{self.postgres_db}")

    def redis_kwargs(self) -> dict:
        return dict(host=self.redis_host, port=self.redis_port, db=self.redis_db,
                    password=self._require(self.redis_password, "AS_REDIS_PASSWORD"))

    def secret(self, name: str) -> str:
        """Value of a required secret field, e.g. secret('upstream_token'); raises if unset."""
        return self._require(getattr(self, name), f"AS_{name.upper()}")

    def redacted(self) -> dict:
        """Settings safe to print/log (secrets masked)."""
        out = self.model_dump()
        for k in ("postgres_password", "redis_password", "upstream_token", "internal_token", "demo_user_password"):
            out[k] = "***" if out.get(k) else None
        return out


@lru_cache
def get_settings() -> Settings:
    return Settings()
