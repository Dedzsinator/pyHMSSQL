"""
CRDT (Conflict-free Replicated Data Types) implementation
Supporting LWW-Element-Set, OR-Set, Vector Clocks, and Hybrid Logical Clocks
"""

import time
import json
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Set, Tuple, List, Union
from dataclasses import dataclass, field
from collections import defaultdict
import threading
import uuid


class Clock(ABC):
    """Abstract base class for logical clocks"""

    @abstractmethod
    def tick(self) -> Any:
        """Increment the clock and return new timestamp"""
        pass

    @abstractmethod
    def update(self, other_time: Any) -> Any:
        """Update clock based on received timestamp"""
        pass

    @abstractmethod
    def compare(self, other_time: Any) -> int:
        """Compare timestamps: -1 (less), 0 (concurrent), 1 (greater)"""
        pass


class VectorClock(Clock):
    """Vector Clock implementation for causal ordering"""

    def __init__(self, node_id: str, initial_clock: Optional[Dict[str, int]] = None):
        self.node_id = node_id
        self.clock = initial_clock or {node_id: 0}
        self._lock = threading.RLock()

    def tick(self) -> Dict[str, int]:
        """Increment local clock"""
        with self._lock:
            self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
            return self.clock.copy()

    def update(self, other_clock: Dict[str, int]) -> Dict[str, int]:
        """Update clock based on received vector clock"""
        with self._lock:
            # Take maximum of each component (including local node)
            for node_id, timestamp in other_clock.items():
                self.clock[node_id] = max(self.clock.get(node_id, 0), timestamp)

            return self.clock.copy()

    def compare(self, time1: Dict[str, int], time2: Dict[str, int]) -> int:
        """Compare two vector clock timestamps"""
        all_nodes = set(time1.keys()) | set(time2.keys())

        time1_greater = False
        time2_greater = False

        for node_id in all_nodes:
            val1 = time1.get(node_id, 0)
            val2 = time2.get(node_id, 0)

            if val1 > val2:
                time1_greater = True
            elif val1 < val2:
                time2_greater = True

        if time1_greater and not time2_greater:
            return 1  # time1 > time2
        elif time2_greater and not time1_greater:
            return -1  # time1 < time2
        elif not time1_greater and not time2_greater:
            return 0  # equal
        else:
            return 0  # concurrent

    def to_dict(self) -> Dict[str, int]:
        """Serialize to dictionary"""
        with self._lock:
            return self.clock.copy()

    @classmethod
    def from_dict(cls, data: Dict[str, int], node_id: str) -> "VectorClock":
        """Deserialize from dictionary"""
        return cls(node_id, data)


@dataclass
class HLCTimestamp:
    """Hybrid Logical Clock timestamp"""

    logical: float = 0
    physical: float = 0
    counter: int = 0
    node_id: str = ""

    def __post_init__(self):
        if self.physical == 0:
            self.physical = time.time()

    def __getitem__(self, key: str):
        """Support dictionary-like access"""
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        """Support dictionary-like assignment"""
        setattr(self, key, value)

    def get(self, key: str, default=None):
        """Dictionary-like get method"""
        return getattr(self, key, default)

    def __gt__(self, other: "HLCTimestamp") -> bool:
        """Greater than comparison for HLC timestamps"""
        if self.physical != other.physical:
            return self.physical > other.physical
        if self.logical != other.logical:
            return self.logical > other.logical
        return self.counter > other.counter

    def __lt__(self, other: "HLCTimestamp") -> bool:
        """Less than comparison for HLC timestamps"""
        if self.physical != other.physical:
            return self.physical < other.physical
        if self.logical != other.logical:
            return self.logical < other.logical
        return self.counter < other.counter

    def __eq__(self, other) -> bool:
        """Equality comparison for HLC timestamps"""
        if isinstance(other, (int, float)):
            return self.logical == other
        elif isinstance(other, HLCTimestamp):
            return (
                self.physical == other.physical
                and self.logical == other.logical
                and self.counter == other.counter
            )
        return False

    def __str__(self) -> str:
        """String representation including node_id"""
        return f"HLCTimestamp(logical={self.logical}, physical={self.physical}, counter={self.counter}, node_id={self.node_id})"


class HybridLogicalClock(Clock):
    """Hybrid Logical Clock (HLC) implementation"""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.logical = 0
        self.counter = 0
        self.last_physical = 0  # Start from 0 so first tick will advance time
        self._lock = threading.RLock()

    @property
    def logical_time(self) -> float:
        """Get current logical time"""
        with self._lock:
            return self.logical

    def tick(self) -> "HLCTimestamp":
        """Generate new HLC timestamp"""
        with self._lock:
            current_physical = time.time()

            if current_physical > self.last_physical:
                self.logical = current_physical
                self.counter = 0
                self.last_physical = current_physical
            else:
                self.logical = max(self.logical, self.last_physical)
                self.counter += 1

            return HLCTimestamp(
                physical=current_physical,
                logical=self.logical,
                counter=self.counter,
                node_id=self.node_id,
            )

    def update(self, other_timestamp: Union[dict, "HLCTimestamp"]) -> "HLCTimestamp":
        """Update HLC with received timestamp"""
        with self._lock:
            current_physical = time.time()

            # Handle both dict and HLCTimestamp objects
            if isinstance(other_timestamp, dict):
                other_physical = other_timestamp.get("physical", 0)
                other_logical = other_timestamp.get("logical", 0)
                other_counter = other_timestamp.get("counter", 0)
            else:
                other_physical = other_timestamp.physical
                other_logical = other_timestamp.logical
                other_counter = other_timestamp.counter

            # Update logical time to max of current physical and logical times
            max_physical = max(current_physical, other_physical)

            if current_physical > max(self.logical, other_logical):
                # Current physical time is greater than all logical times
                self.logical = current_physical
                self.counter = 0
            elif other_logical > self.logical:
                # Other logical time is greater
                self.logical = other_logical
                self.counter = other_counter + 1
            elif other_logical == self.logical:
                # Same logical time, increment counter
                self.counter = max(self.counter, other_counter) + 1
            else:
                # Our logical time is greater, just increment counter
                self.counter += 1

            self.last_physical = max_physical

            return HLCTimestamp(
                physical=current_physical,
                logical=self.logical,
                counter=self.counter,
                node_id=self.node_id,
            )

    def compare(self, timestamp1: dict, timestamp2: dict) -> int:
        """Compare two HLC timestamps"""
        # Compare physical time first
        if timestamp1["physical"] != timestamp2["physical"]:
            return 1 if timestamp1["physical"] > timestamp2["physical"] else -1

        # Then logical time
        if timestamp1["logical"] != timestamp2["logical"]:
            return 1 if timestamp1["logical"] > timestamp2["logical"] else -1

        # Finally counter
        if timestamp1["counter"] != timestamp2["counter"]:
            return 1 if timestamp1["counter"] > timestamp2["counter"] else -1

        return 0


class CRDTValue(ABC):
    """Abstract base class for CRDT values"""

    @abstractmethod
    def merge(self, other: "CRDTValue") -> "CRDTValue":
        """Merge with another CRDT value"""
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        pass

    def serialize(self) -> str:
        """Serialize to JSON string"""
        return json.dumps(self.to_dict())

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CRDTValue":
        """Deserialize from dictionary"""
        pass


@dataclass
class LWWElement:
    """Last-Writer-Wins element with timestamp"""

    value: Any
    timestamp: Union[Dict[str, int], HLCTimestamp]
    node_id: str
    deleted: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "timestamp": (
                self.timestamp.__dict__
                if hasattr(self.timestamp, "__dict__")
                else self.timestamp
            ),
            "node_id": self.node_id,
            "deleted": self.deleted,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LWWElement":
        timestamp = data["timestamp"]
        if isinstance(timestamp, dict) and "logical" in timestamp:
            timestamp = HLCTimestamp(**timestamp)
        return cls(
            value=data["value"],
            timestamp=timestamp,
            node_id=data["node_id"],
            deleted=data.get("deleted", False),
        )


class LWWElementSet(CRDTValue):
    """Last-Writer-Wins Element Set CRDT"""

    def __init__(self, clock: Clock, node_id: str):
        self.clock = clock
        self.node_id = node_id
        self._elements: Dict[Any, LWWElement] = {}  # Use underscore to avoid conflict
        self._lock = threading.RLock()

    def add(self, value: Any) -> None:
        """Add element to set"""
        with self._lock:
            timestamp = self.clock.tick()
            element = LWWElement(value, timestamp, self.node_id, deleted=False)
            self._elements[value] = element

    def remove(self, value: Any) -> None:
        """Remove element from set"""
        with self._lock:
            timestamp = self.clock.tick()
            if value in self._elements:
                # Update existing element
                self._elements[value].deleted = True
                self._elements[value].timestamp = timestamp
                self._elements[value].node_id = self.node_id
            else:
                # Create tombstone
                element = LWWElement(value, timestamp, self.node_id, deleted=True)
                self._elements[value] = element

    def contains(self, value: Any) -> bool:
        """Check if element is in set"""
        with self._lock:
            element = self._elements.get(value)
            return element is not None and not element.deleted

    def value(self) -> set:
        """Get the current value as a set of all non-deleted elements"""
        with self._lock:
            return {
                element.value
                for element in self._elements.values()
                if not element.deleted
            }

    @property
    def elements(self) -> Dict[Any, LWWElement]:
        """Get the internal elements dictionary for backward compatibility"""
        return self._elements

    def get_value(self) -> Any:
        """Get the current value (for single-value LWW register)"""
        with self._lock:
            if not self._elements:
                return None

            # Find the most recent non-deleted element
            latest_element = None
            for element in self._elements.values():
                if not element.deleted:
                    if (
                        latest_element is None
                        or self._compare_timestamps(
                            element.timestamp, latest_element.timestamp
                        )
                        > 0
                    ):
                        latest_element = element

            return latest_element.value if latest_element else None

    def set_value(self, value: Any) -> None:
        """Set value (for single-value LWW register)"""
        with self._lock:
            timestamp = self.clock.tick()
            # Use a fixed key for single-value register
            element = LWWElement(value, timestamp, self.node_id, deleted=False)
            self._elements["__value__"] = element

    def _compare_timestamps(self, ts1: Any, ts2: Any) -> int:
        """Compare timestamps using the appropriate method"""
        if hasattr(ts1, "__dict__") and hasattr(ts2, "__dict__"):  # HLC timestamps
            if ts1.physical < ts2.physical:
                return -1
            elif ts1.physical > ts2.physical:
                return 1
            else:
                if ts1.logical < ts2.logical:
                    return -1
                elif ts1.logical > ts2.logical:
                    return 1
                else:
                    return 0
        elif isinstance(ts1, dict) and isinstance(ts2, dict):  # Vector clocks
            # Use vector clock comparison logic
            return self.clock.compare(ts1, ts2) if hasattr(self.clock, "compare") else 0
        else:
            # Fallback to simple comparison
            return -1 if ts1 < ts2 else (1 if ts1 > ts2 else 0)

    def merge(self, other) -> None:
        """Merge with another LWW set in-place"""
        with self._lock:
            # Handle both LWWElementSet objects and dict states
            if isinstance(other, dict):
                # Convert dict state to temporary LWWElementSet for merging
                temp_crdt = LWWElementSet.from_dict(other, self.clock, self.node_id)
                other_elements = temp_crdt._elements
            else:
                other_elements = other._elements
            
            # Merge elements from other
            for value, other_element in other_elements.items():
                if value not in self._elements:
                    self._elements[value] = other_element
                else:
                    # Keep the element with later timestamp
                    self_element = self._elements[value]
                    if (
                        self._compare_timestamps(
                            other_element.timestamp, self_element.timestamp
                        )
                        > 0
                    ):
                        self._elements[value] = other_element
                    elif (
                        self._compare_timestamps(
                            other_element.timestamp, self_element.timestamp
                        )
                        == 0
                    ):
                        # Same timestamp, use node_id as tiebreaker
                        if other_element.node_id > self_element.node_id:
                            self._elements[value] = other_element

    def get_state(self) -> Dict[str, Any]:
        """Get state for merging"""
        return self.to_dict()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        with self._lock:
            return {
                "type": "lww_element_set",
                "node_id": self.node_id,
                "elements": {str(k): v.to_dict() for k, v in self._elements.items()},
            }

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], clock: Clock, node_id: str
    ) -> "LWWElementSet":
        """Deserialize from dictionary"""
        result = cls(clock, node_id)

        for key, element_data in data.get("elements", {}).items():
            # Try to convert key back to original type
            try:
                # Try common types
                if key.isdigit():
                    actual_key = int(key)
                elif key.replace(".", "").isdigit():
                    actual_key = float(key)
                else:
                    actual_key = key
            except:
                actual_key = key

            result._elements[actual_key] = LWWElement.from_dict(element_data)

        return result


class ORSet(CRDTValue):
    """Observed-Remove Set CRDT"""

    def __init__(self, clock_or_node_id):
        # Handle both clock object and node_id string for backward compatibility
        if isinstance(clock_or_node_id, str):
            self.node_id = clock_or_node_id
            self.clock = None
        else:
            # Assume it's a clock object with node_id attribute
            self.clock = clock_or_node_id
            self.node_id = clock_or_node_id.node_id
            
        self.added_elements: List[Tuple[Any, str]] = (
            []
        )  # List of (element, unique_tag) pairs
        self.removed_tags: Set[str] = set()  # Set of removed tags
        self._element_tags: Dict[Any, Set[str]] = defaultdict(
            set
        )  # element -> set of tags for quick lookup

        # Compatibility attributes for to_dict/from_dict methods
        self.added: Dict[str, Set[str]] = defaultdict(
            set
        )  # element -> set of added tags
        self.removed: Dict[str, Set[str]] = defaultdict(
            set
        )  # element -> set of removed tags

        self._lock = threading.RLock()

    def add(self, value: Any) -> None:
        """Add element to set"""
        with self._lock:
            # Generate unique tag
            tag = f"{self.node_id}:{uuid.uuid4()}"
            self.added_elements.append((value, tag))
            self._element_tags[value].add(tag)
            # Update compatibility attributes
            self.added[str(value)].add(tag)

    def remove(self, value: Any) -> None:
        """Remove element from set"""
        with self._lock:
            # Remove all currently added tags for this element
            if value in self._element_tags:
                for tag in self._element_tags[value]:
                    self.removed_tags.add(tag)
                    # Update compatibility attributes
                    self.removed[str(value)].add(tag)

    def contains(self, value: Any) -> bool:
        """Check if element is in set"""
        with self._lock:
            added_tags = self._element_tags.get(value, set())
            return len(added_tags - self.removed_tags) > 0

    def elements(self) -> Set[Any]:
        """Get all elements in the set"""
        with self._lock:
            result = set()
            for value in self._element_tags:
                if self.contains(value):
                    result.add(value)
            return result

    def values(self) -> Set[Any]:
        """Get all values in the set (alias for elements)"""
        return self.elements()

    def get_state(self) -> Dict[str, Any]:
        """Get state for merging"""
        with self._lock:
            return {
                "type": "or_set",
                "node_id": self.node_id,
                "added_elements": [
                    (str(elem), tag) for elem, tag in self.added_elements
                ],
                "removed_tags": list(self.removed_tags),
            }

    def merge(self, other) -> None:
        """Merge with another OR set in-place"""
        with self._lock:
            # Handle both ORSet objects and dict states
            if isinstance(other, dict):
                other_added = other.get("added_elements", [])
                other_removed = set(other.get("removed_tags", []))
            else:
                other_added = other.added_elements
                other_removed = other.removed_tags

            # Merge added elements
            for item in other_added:
                if isinstance(item, tuple):
                    element, tag = item
                else:
                    # Handle dict format
                    element, tag = item[0], item[1]

                # Try to convert element back to original type
                try:
                    if isinstance(element, str) and element.isdigit():
                        element = int(element)
                    elif (
                        isinstance(element, str) and element.replace(".", "").isdigit()
                    ):
                        element = float(element)
                except:
                    pass

                if (element, tag) not in self.added_elements:
                    self.added_elements.append((element, tag))
                    self._element_tags[element].add(tag)

            # Merge removed tags
            self.removed_tags.update(other_removed)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        with self._lock:
            return {
                "type": "or_set",
                "node_id": self.node_id,
                "added": {str(k): list(v) for k, v in self.added.items()},
                "removed": {str(k): list(v) for k, v in self.removed.items()},
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ORSet":
        """Deserialize from dictionary"""
        result = cls(data["node_id"])

        for key, tags in data.get("added", {}).items():
            result.added[key] = set(tags)

        for key, tags in data.get("removed", {}).items():
            result.removed[key] = set(tags)

        return result


class CRDTCounter(CRDTValue):
    """CRDT Counter using PN-Counter (increment/decrement)"""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.increments: Dict[str, int] = defaultdict(int)
        self.decrements: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

    def increment(self, amount: int = 1) -> None:
        """Increment counter"""
        with self._lock:
            self.increments[self.node_id] += amount

    def decrement(self, amount: int = 1) -> None:
        """Decrement counter"""
        with self._lock:
            self.decrements[self.node_id] += amount

    def value(self) -> int:
        """Get current counter value"""
        with self._lock:
            total_increments = sum(self.increments.values())
            total_decrements = sum(self.decrements.values())
            return total_increments - total_decrements

    def merge(self, other: "CRDTCounter") -> "CRDTCounter":
        """Merge with another counter"""
        result = CRDTCounter(self.node_id)

        with self._lock:
            # Merge increments
            all_nodes = set(self.increments.keys()) | set(other.increments.keys())
            for node in all_nodes:
                result.increments[node] = max(
                    self.increments.get(node, 0), other.increments.get(node, 0)
                )

            # Merge decrements
            all_nodes = set(self.decrements.keys()) | set(other.decrements.keys())
            for node in all_nodes:
                result.decrements[node] = max(
                    self.decrements.get(node, 0), other.decrements.get(node, 0)
                )

        return result

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        with self._lock:
            return {
                "type": "crdt_counter",
                "node_id": self.node_id,
                "increments": dict(self.increments),
                "decrements": dict(self.decrements),
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CRDTCounter":
        """Deserialize from dictionary"""
        result = cls(data["node_id"])
        result.increments = defaultdict(int, data.get("increments", {}))
        result.decrements = defaultdict(int, data.get("decrements", {}))
        return result


def create_crdt_value(
    value_type: str,
    node_id: str,
    clock: Optional[Clock] = None,
    initial_data: Optional[Dict[str, Any]] = None,
) -> CRDTValue:
    """Factory function to create CRDT values"""
    if value_type in ["lww_register", "lww_set", "lww_element_set"]:
        if clock is None:
            clock = HybridLogicalClock(node_id)
        if initial_data:
            return LWWElementSet.from_dict(initial_data, clock, node_id)
        return LWWElementSet(clock, node_id)

    elif value_type == "or_set":
        if initial_data:
            return ORSet.from_dict(initial_data)
        return ORSet(node_id)

    elif value_type == "counter":
        if initial_data:
            return CRDTCounter.from_dict(initial_data)
        return CRDTCounter(node_id)

    else:
        raise ValueError(f"Unknown CRDT type: {value_type}")


def create_crdt(
    crdt_type: str,
    value: Any = None,
    node_id: str = "default",
    timestamp: Optional[float] = None,
) -> CRDTValue:
    """
    Simplified factory function to create CRDT values with initial data.

    Args:
        crdt_type: Type of CRDT (lww, orset, counter)
        value: Initial value to store
        node_id: Node identifier
        timestamp: Optional timestamp

    Returns:
        CRDTValue instance
    """
    # Map common aliases
    type_mapping = {
        "lww": "lww_register",
        "lww_set": "lww_set",
        "orset": "or_set",
        "or_set": "or_set",
        "counter": "counter",
    }

    actual_type = type_mapping.get(crdt_type, crdt_type)

    if actual_type in ["lww_register", "lww_set"]:
        clock = HybridLogicalClock(node_id)
        if timestamp:
            # Create HLC timestamp from provided timestamp
            if isinstance(timestamp, (int, float)):
                hlc_timestamp = HLCTimestamp(
                    logical=0, physical=int(timestamp * 1000000)
                )
                # Update the clock with this timestamp
                clock.update(hlc_timestamp)
            elif hasattr(timestamp, "logical") and hasattr(timestamp, "physical"):
                # It's already an HLCTimestamp
                clock.update(timestamp)

        crdt = LWWElementSet(clock, node_id)
        if value is not None:
            crdt.add(value)
        return crdt

    elif actual_type == "or_set":
        crdt = ORSet(node_id)
        if value is not None:
            if isinstance(value, (list, set)):
                for item in value:
                    crdt.add(item)
            else:
                crdt.add(value)
        return crdt

    elif actual_type == "counter":
        crdt = CRDTCounter(node_id)
        if value is not None and isinstance(value, (int, float)):
            crdt.increment(value)
        return crdt

    else:
        raise ValueError(f"Unknown CRDT type: {crdt_type}")


class GCounter(CRDTValue):
    """G-Counter (Grow-only Counter) implementation"""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.counters: Dict[str, int] = defaultdict(int)
        self.counters[node_id] = 0
        self._lock = threading.RLock()

    def increment(self, amount: int = 1) -> None:
        """Increment counter"""
        with self._lock:
            self.counters[self.node_id] += amount

    def value(self) -> int:
        """Get current counter value"""
        with self._lock:
            return sum(self.counters.values())

    def merge(self, other) -> None:
        """Merge with another G-Counter"""
        with self._lock:
            # Handle both GCounter objects and dict states
            if isinstance(other, dict):
                other_counters = other.get("counters", {})
            else:
                other_counters = other.counters

            for node_id, count in other_counters.items():
                self.counters[node_id] = max(self.counters[node_id], count)

    def get_state(self) -> Dict[str, Any]:
        """Get state for merging"""
        with self._lock:
            return {
                "type": "g_counter",
                "node_id": self.node_id,
                "counters": dict(self.counters),
            }

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        return self.get_state()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GCounter":
        """Deserialize from dictionary"""
        result = cls(data["node_id"])
        result.counters = defaultdict(int, data.get("counters", {}))
        return result


class PNCounter(CRDTValue):
    """PN-Counter (Increment/Decrement Counter) implementation"""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.p_counter = GCounter(node_id)  # Positive counter
        self.n_counter = GCounter(node_id)  # Negative counter
        self._lock = threading.RLock()

    def increment(self, amount: int = 1) -> None:
        """Increment counter"""
        with self._lock:
            self.p_counter.increment(amount)

    def decrement(self, amount: int = 1) -> None:
        """Decrement counter"""
        with self._lock:
            self.n_counter.increment(amount)

    def value(self) -> int:
        """Get current counter value"""
        with self._lock:
            return self.p_counter.value() - self.n_counter.value()

    def merge(self, other) -> None:
        """Merge with another PN-Counter"""
        with self._lock:
            # Handle both PNCounter objects and dict states
            if isinstance(other, dict):
                if "p_counter" in other:
                    self.p_counter.merge(other["p_counter"])
                if "n_counter" in other:
                    self.n_counter.merge(other["n_counter"])
            else:
                self.p_counter.merge(other.p_counter)
                self.n_counter.merge(other.n_counter)

    def get_state(self) -> Dict[str, Any]:
        """Get state for merging"""
        with self._lock:
            return {
                "type": "pn_counter",
                "node_id": self.node_id,
                "p_counter": self.p_counter.get_state(),
                "n_counter": self.n_counter.get_state(),
            }

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        return self.get_state()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PNCounter":
        """Deserialize from dictionary"""
        result = cls(data["node_id"])
        if "p_counter" in data:
            result.p_counter = GCounter.from_dict(data["p_counter"])
        if "n_counter" in data:
            result.n_counter = GCounter.from_dict(data["n_counter"])
        return result


class CRDTManager:
    """Manager for coordinating multiple CRDTs"""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock = VectorClock(node_id)
        self.crdts: Dict[str, CRDTValue] = {}
        self._lock = threading.RLock()

    def create_lww_set(self, name: str) -> LWWElementSet:
        """Create a new LWW Element Set"""
        with self._lock:
            lww_set = LWWElementSet(self.clock, self.node_id)
            self.crdts[name] = lww_set
            return lww_set

    def create_or_set(self, name: str) -> ORSet:
        """Create a new OR Set"""
        with self._lock:
            or_set = ORSet(self.node_id)
            or_set.clock = self.clock  # Link to the manager's clock
            self.crdts[name] = or_set
            return or_set

    def create_g_counter(self, name: str) -> GCounter:
        """Create a new G-Counter"""
        with self._lock:
            counter = GCounter(self.node_id)
            self.crdts[name] = counter
            return counter

    def create_pn_counter(self, name: str) -> PNCounter:
        """Create a new PN-Counter"""
        with self._lock:
            counter = PNCounter(self.node_id)
            self.crdts[name] = counter
            return counter

    def get_crdt(self, name: str) -> Optional[CRDTValue]:
        """Get a CRDT by name"""
        with self._lock:
            return self.crdts.get(name)

    def get_state(self) -> Dict[str, Any]:
        """Get the state of all CRDTs"""
        with self._lock:
            state = {"clock": self.clock.clock.copy()}
            for name, crdt in self.crdts.items():
                state[name] = crdt.get_state()
            return state

    def merge_state(self, other_state: Dict[str, Any]) -> None:
        """Merge state from another manager"""
        with self._lock:
            # Update clock
            if "clock" in other_state:
                self.clock.update(other_state["clock"])

            # Merge CRDTs
            for name, crdt_state in other_state.items():
                if name == "clock":
                    continue

                if name in self.crdts:
                    # Merge existing CRDT
                    existing_crdt = self.crdts[name]
                    if hasattr(existing_crdt, "merge"):
                        # Create temporary CRDT from state for merging
                        crdt_type = crdt_state.get("type")
                        if crdt_type == "lww_element_set":
                            temp_crdt = LWWElementSet.from_dict(
                                crdt_state, existing_crdt.clock, existing_crdt.node_id
                            )
                            existing_crdt.merge(temp_crdt)
                        elif crdt_type == "or_set":
                            temp_crdt = ORSet.from_dict(crdt_state)
                            existing_crdt.merge(temp_crdt)
                        elif crdt_type == "g_counter":
                            temp_crdt = GCounter.from_dict(crdt_state)
                            existing_crdt.merge(temp_crdt)
                        elif crdt_type == "pn_counter":
                            temp_crdt = PNCounter.from_dict(crdt_state)
                            existing_crdt.merge(temp_crdt)
                else:
                    # Create new CRDT from state
                    crdt_type = crdt_state.get("type")
                    if crdt_type == "lww_element_set":
                        hlc = HybridLogicalClock(self.node_id)
                        self.crdts[name] = LWWElementSet.from_dict(
                            crdt_state, hlc, self.node_id
                        )
                    elif crdt_type == "or_set":
                        self.crdts[name] = ORSet.from_dict(crdt_state)
                    elif crdt_type == "g_counter":
                        self.crdts[name] = GCounter.from_dict(crdt_state)
                    elif crdt_type == "pn_counter":
                        self.crdts[name] = PNCounter.from_dict(crdt_state)
