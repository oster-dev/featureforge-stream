"""Validated input contract for interaction-events.v1."""

from datetime import UTC, datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

EntityId = Annotated[str, Field(strict=True, min_length=1, pattern=r"^\S+$")]
PlaySeconds = Annotated[int, Field(strict=True, ge=0)]


class InteractionEventV1(BaseModel):
    """One synthetic interaction with explicit identity and event time."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_time: AwareDatetime
    ingested_at: AwareDatetime
    user_id: EntityId
    content_id: EntityId
    event_type: Literal["impression", "play", "skip", "complete"]
    play_seconds: PlaySeconds
    source: Literal["web", "mobile", "tv"]
    schema_version: Literal["v1"]

    @field_validator("event_id", mode="before")
    @classmethod
    def require_uuid_input(cls, value: object) -> object:
        if not isinstance(value, (str, UUID)):
            # Pydantic must report invalid input as a ValidationError.
            raise ValueError(  # noqa: TRY004
                "event_id must be a UUID string or UUID instance"
            )
        return value

    @field_validator("event_time", "ingested_at", mode="before")
    @classmethod
    def require_timestamp_input(cls, value: object) -> object:
        if not isinstance(value, (str, datetime)):
            # Pydantic must report invalid input as a ValidationError.
            raise ValueError(  # noqa: TRY004
                "timestamps must be strings or datetime instances"
            )

        if isinstance(value, str):
            if "T" not in value:
                raise ValueError("timestamp strings must contain a T separator")
            try:
                datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError("timestamp must be an ISO 8601 datetime") from exc

        return value

    @field_validator("event_time", "ingested_at")
    @classmethod
    def normalize_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_event_semantics(self) -> Self:
        if self.ingested_at < self.event_time:
            raise ValueError("ingested_at must be on or after event_time")

        if self.event_type == "impression" and self.play_seconds != 0:
            raise ValueError("impression events must have play_seconds equal to zero")

        return self
