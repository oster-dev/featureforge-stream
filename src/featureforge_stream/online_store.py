"""Validated Redis storage with observation-time guarded writes."""

from datetime import UTC, timedelta
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from redis import Redis
from redis.exceptions import WatchError

WriteResult = Literal["written", "unchanged", "stale"]


class FeatureWriteConflictError(ValueError):
    """Different values target the same observation window."""


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
    """Store one feature per user with guarded transactional updates.

    Older observation windows cannot replace newer ones. Identical retries
    are no-ops. Different values for the same window are explicit conflicts
    until a correction-ordering contract is introduced.

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

    @staticmethod
    def _decode(
        payload: str | bytes,
        *,
        user_id: str,
    ) -> UserPlayCountFeatureV1:
        feature = UserPlayCountFeatureV1.model_validate_json(payload)
        if feature.user_id != user_id:
            raise ValueError("stored feature user_id does not match requested user_id")

        return feature

    def write(self, feature: UserPlayCountFeatureV1) -> WriteResult:
        """Compare and write using optimistic locking.

        Retry at most 16 times when concurrent writers change the key.
        Connection errors and exhausted retries propagate to the caller.
        """
        key = self.key_for(feature.user_id)
        payload = feature.model_dump_json()

        for _ in range(16):
            with self._client.pipeline() as pipeline:
                try:
                    pipeline.watch(key)
                    stored_payload = pipeline.get(key)

                    if stored_payload is not None:
                        stored = self._decode(
                            stored_payload,
                            user_id=feature.user_id,
                        )

                        if feature.observation_time < stored.observation_time:
                            pipeline.unwatch()
                            return "stale"

                        if feature.observation_time == stored.observation_time:
                            if feature == stored:
                                pipeline.unwatch()
                                return "unchanged"

                            raise FeatureWriteConflictError(
                                "Different feature values for the same observation_time: "
                                f"user_id={feature.user_id!r}, "
                                f"observation_time={feature.observation_time.isoformat()}"
                            )

                    pipeline.multi()
                    pipeline.set(key, payload)
                    pipeline.execute()
                    return "written"
                except WatchError:
                    continue

        raise WatchError("Feature write aborted after 16 concurrent modification retries")

    def read(self, user_id: str) -> UserPlayCountFeatureV1 | None:
        """Read a validated feature, or return None when no key exists."""
        payload = self._client.get(self.key_for(user_id))
        if payload is None:
            return None

        return self._decode(payload, user_id=user_id)
