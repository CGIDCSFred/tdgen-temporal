"""Base class and types for all entity state machines."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class SideEffect:
    """A mutation to apply to another entity as a result of a state transition."""
    table:     str
    pk_col:    str
    pk_val:    Any
    updates:   dict[str, Any]


@dataclass
class AdvanceResult:
    updated_row:    dict
    changed_fields: list[str]
    side_effects:   list[SideEffect] = field(default_factory=list)
    new_rows:       dict[str, list[dict]] = field(default_factory=dict)


class StateMachine(ABC):
    """
    All state machines implement advance() which takes one entity row
    and returns an AdvanceResult describing mutations and side effects.
    """

    @abstractmethod
    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: "random.Random",
    ) -> AdvanceResult:
        ...
