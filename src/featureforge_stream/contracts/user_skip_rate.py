"""Payload and append-only update policy for user_skip_rate_24h."""

from datetime import UTC, timedelta
from math import isclose
from typing import Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

UpdateDecision = Literal["written", "unchanged", "stale"]


class SkipRateUpdateConflictError(ValueError):
    """Same-window count vectors cannot be ordered componentwise."""


class UserSkipRateFeatureV1(BaseModel):
    """One user/window result with explicit numerator and denominator inputs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feature_schema_version: Literal["v1"] = "v1"
    feature_name: Literal["user_skip_rate_24h"] = "user_skip_rate_24h"
    user_id: str = Field(strict=True, min_length=1)
    play_count: int = Field(strict=True, ge=0)
    skip_count: int = Field(strict=True, ge=0)
    skip_rate: float = Field(strict=True, ge=0, le=1, allow_inf_nan=False)
    observation_time: AwareDatetime
    window_start_exclusive: AwareDatetime

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, value: str) -> str:
        if any(character.isspace() for character in value):
            raise ValueError("user_id must contain no whitespace")
        return value

    @field_validator("observation_time", "window_start_exclusive")
    @classmethod
    def normalize_timestamp(cls, value: AwareDatetime) -> AwareDatetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_feature(self) -> Self:
        if self.observation_time - self.window_start_exclusive != timedelta(hours=24):
            raise ValueError("feature window must be exactly 24 hours")

        denominator = self.play_count + self.skip_count
        expected = self.skip_count / denominator if denominator else 0.0
        if not isclose(self.skip_rate, expected, rel_tol=1e-12, abs_tol=1e-15):
            raise ValueError("skip_rate must match skip_count / (play_count + skip_count)")
        return self


def decide_skip_rate_update(
    stored: UserSkipRateFeatureV1 | None,
    incoming: UserSkipRateFeatureV1,
) -> UpdateDecision:
    """Order windows, then same-window append-only count vectors.

    Callers must isolate independent runs in separate namespaces.
    This does not prove event-set identity or repair inflated counts.
    """
    if stored is None:
        return "written"
    if incoming.user_id != stored.user_id:
        raise ValueError("cannot compare features for different user IDs")

    if incoming.observation_time > stored.observation_time:
        return "written"
    if incoming.observation_time < stored.observation_time:
        return "stale"

    current = (stored.play_count, stored.skip_count)
    candidate = (incoming.play_count, incoming.skip_count)

    if candidate == current:
        return "unchanged"
    if all(new >= old for new, old in zip(candidate, current)):
        return "written"
    if all(new <= old for new, old in zip(candidate, current)):
        return "stale"

    raise SkipRateUpdateConflictError(
        f"Incomparable counts for user_id={incoming.user_id}, "
        f"observation_time={incoming.observation_time.isoformat()}: "
        f"stored={current}, incoming={candidate}"
    )
