"""Guarded Redis writes for append-only user skip-rate count vectors."""

from redis import Redis
from redis.exceptions import WatchError

from featureforge_stream.contracts.user_skip_rate import (
    UpdateDecision,
    UserSkipRateFeatureV1,
    decide_skip_rate_update,
)


class RedisUserSkipRateStore:
    """Store the latest window within one consistent run namespace.

    Same-window updates follow componentwise count ordering, not rate ordering.
    Independent runs require independent namespaces. Retractions are unsupported.
    The caller owns the Redis connection lifecycle.
    """

    def __init__(self, client: Redis, *, key_prefix: str) -> None:
        if not isinstance(key_prefix, str):
            raise TypeError("key_prefix must be a string")
        if not key_prefix or any(character.isspace() for character in key_prefix):
            raise ValueError("key_prefix must be non-empty and contain no whitespace")
        self._client = client
        self._key_prefix = key_prefix

    def key_for(self, user_id: str) -> str:
        if not isinstance(user_id, str):
            raise TypeError("user_id must be a string")
        if not user_id or any(character.isspace() for character in user_id):
            raise ValueError("user_id must be non-empty and contain no whitespace")
        return f"{self._key_prefix}:{user_id}"

    @staticmethod
    def _decode(payload: str | bytes, *, user_id: str) -> UserSkipRateFeatureV1:
        feature = UserSkipRateFeatureV1.model_validate_json(payload)
        if feature.user_id != user_id:
            raise ValueError("stored feature user_id does not match requested user_id")
        return feature

    def read(self, user_id: str) -> UserSkipRateFeatureV1 | None:
        payload = self._client.get(self.key_for(user_id))
        if payload is None:
            return None
        return self._decode(payload, user_id=user_id)

    def write(self, feature: UserSkipRateFeatureV1) -> UpdateDecision:
        if not isinstance(feature, UserSkipRateFeatureV1):
            raise TypeError("feature must be a UserSkipRateFeatureV1 instance")

        key = self.key_for(feature.user_id)
        payload = feature.model_dump_json()

        for _ in range(16):
            with self._client.pipeline() as pipeline:
                try:
                    pipeline.watch(key)
                    stored_payload = pipeline.get(key)
                    stored = (
                        self._decode(stored_payload, user_id=feature.user_id)
                        if stored_payload is not None
                        else None
                    )
                    decision = decide_skip_rate_update(stored, feature)
                    if decision != "written":
                        pipeline.unwatch()
                        return decision

                    pipeline.multi()
                    pipeline.set(key, payload)
                    pipeline.execute()
                    return "written"
                except WatchError:
                    continue

        raise WatchError("Feature write aborted after 16 concurrent modification retries")
