import os
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class Config:
    """Application configuration from environment variables."""

    role: str  # "leader" or "follower"
    port: int
    followers: List[str]  # ["host:port", ...] (leader only)
    write_quorum: int  # (leader only)
    min_delay: float  # seconds (leader only)
    max_delay: float  # seconds (leader only)
    repl_secret: Optional[str]  # Optional shared secret
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        role = os.getenv("ROLE", "follower").lower()
        port = int(os.getenv("PORT", "8000"))

        # Leader-specific config
        followers_str = os.getenv("FOLLOWERS", "")
        followers = (
            [f.strip() for f in followers_str.split(",") if f.strip()]
            if followers_str
            else []
        )
        write_quorum = int(os.getenv("WRITE_QUORUM", "1"))
        min_delay = float(os.getenv("MIN_DELAY", "0.0001"))
        max_delay = float(os.getenv("MAX_DELAY", "0.001"))

        # Common config
        repl_secret = os.getenv("REPL_SECRET")
        log_level = os.getenv("LOG_LEVEL", "INFO")

        # Validation
        if role not in ["leader", "follower"]:
            raise ValueError(f"Invalid ROLE: {role}")

        if write_quorum < 1:
            raise ValueError(f"WRITE_QUORUM must be >= 1, got {write_quorum}")

        if role == "leader":
            if not followers:
                raise ValueError("Leader must have FOLLOWERS configured")
            if write_quorum > len(followers):
                raise ValueError(
                    f"WRITE_QUORUM ({write_quorum}) > followers ({len(followers)})"
                )
        if min_delay < 0 or max_delay < min_delay:
            raise ValueError(f"Invalid delay range: [{min_delay}, {max_delay}]")

        return cls(
            role=role,
            port=port,
            followers=followers,
            write_quorum=write_quorum,
            min_delay=min_delay,
            max_delay=max_delay,
            repl_secret=repl_secret,
            log_level=log_level,
        )
