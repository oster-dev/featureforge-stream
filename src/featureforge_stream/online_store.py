"""Validated Redis storage for user_play_count_1h feature values."""

from datetime import UTC, timedelta
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from redis import Redis


class UserPlayCountFeatureV1(BaseModel):
    """A user play count for one explicit observation window."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feature_schema_version: Literal["v1"] = "v1"
    feature_name: Literal["user_play_count_1h"] = "user_play_count_1h"
    user_id: str = Field(min_length=1)
    play_count: int = Field(ge=0, strict=True)
    observation_time: AwareDatetime
    window_start_exclusive: AwareDatetime

    @field_validator("user_id")
    @classmethod
    def reject_blank_user_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("user_id must not be blank")
        return value

    @field_validator("observation_time", "window_start_exclusive")
    @classmethod
    def normalize_timestamp(cls, value: AwareDatetime) -> AwareDatetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_window(self) -> "UserPlayCountFeatureV1":
        if self.observation_time - self.window_start_exclusive != timedelta(hours=1):
            raise ValueError("feature window must be exactly one hour")
        return self


class RedisUserPlayCountStore:
    """Store and retrieve one feature value per user.

    Writes currently replace any existing value without a timestamp guard.
    The caller owns the Redis client's connection lifecycle.
    """

    def __init__(
        self,
        client: Redis,
        *,
        key_prefix: str = "featureforge-stream:online:v1:user_play_count_1h",
    ) -> None:
        if not key_prefix.strip():
            raise ValueError("key_prefix must not be blank")

        self._client = client
        self._key_prefix = key_prefix

    def key_for(self, user_id: str) -> str:
        """Return the Redis key used for this user."""
        if not user_id.strip():
            raise ValueError("user_id must not be blank")

        return f"{self._key_prefix}:{user_id}"

    def write(self, feature: UserPlayCountFeatureV1) -> None:
        """Write a complete validated feature payload."""
        self._client.set(
            self.key_for(feature.user_id),
            feature.model_dump_json(),
        )

    def read(self, user_id: str) -> UserPlayCountFeatureV1 | None:
        """Read a validated feature, or return None when no key exists."""
        payload = self._client.get(self.key_for(user_id))
        if payload is None:
            return None

        feature = UserPlayCountFeatureV1.model_validate_json(payload)
        if feature.user_id != user_id:
            raise ValueError("stored feature user_id does not match requested user_id")

        return feature
