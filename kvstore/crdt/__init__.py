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
            # Take maximum of each component
            for node_id, timestamp in other_clock.items():
                self.clock[node_id] = max(self.clock.get(node_id, 0), timestamp)
            
            # Increment local clock
            self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
            return self.clock.copy()
    
    def compare(self, other_clock: Dict[str, int]) -> int:
        """Compare vector clocks"""
        with self._lock:
            all_nodes = set(self.clock.keys()) | set(other_clock.keys())
            
            self_greater = False
            other_greater = False
            
            for node_id in all_nodes:
                self_val = self.clock.get(node_id, 0)
                other_val = other_clock.get(node_id, 0)
                
                if self_val > other_val:
                    self_greater = True
                elif self_val < other_val:
                    other_greater = True
            
            if self_greater and not other_greater:
                return 1  # self > other
            elif other_greater and not self_greater:
                return -1  # self < other
            elif not self_greater and not other_greater:
                return 0  # equal
            else:
                return 0  # concurrent
    
    def to_dict(self) -> Dict[str, int]:
        """Serialize to dictionary"""
        with self._lock:
            return self.clock.copy()
    
    @classmethod
    def from_dict(cls, data: Dict[str, int], node_id: str) -> 'VectorClock':
        """Deserialize from dictionary"""
        return cls(node_id, data)


@dataclass
class HLCTimestamp:
    """Hybrid Logical Clock timestamp"""
    logical: int = 0
    physical: int = 0
    node_id: str = ""
    
    def __post_init__(self):
        if self.physical == 0:
            self.physical = int(time.time() * 100)  # 10ms granularity
    
    def __gt__(self, other: 'HLCTimestamp') -> bool:
        """Greater than comparison for HLC timestamps"""
        if self.physical != other.physical:
            return self.physical > other.physical
        return self.logical > other.logical
    
    def __lt__(self, other: 'HLCTimestamp') -> bool:
        """Less than comparison for HLC timestamps"""
        if self.physical != other.physical:
            return self.physical < other.physical
        return self.logical < other.logical
    
    def __eq__(self, other: 'HLCTimestamp') -> bool:
        """Equality comparison for HLC timestamps"""
        return self.physical == other.physical and self.logical == other.logical
    
    def __str__(self) -> str:
        """String representation including node_id"""
        return f"HLCTimestamp(logical={self.logical}, physical={self.physical}, node_id={self.node_id})"


class HybridLogicalClock(Clock):
    """Hybrid Logical Clock (HLC) implementation"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.logical = 0
        self.last_physical = int(time.time() * 100)  # 10ms granularity
        self._lock = threading.RLock()
    
    @property
    def logical_time(self) -> HLCTimestamp:
        """Get current logical timestamp"""
        with self._lock:
            return HLCTimestamp(self.logical, self.last_physical, self.node_id)
    
    def tick(self) -> HLCTimestamp:
        """Generate new HLC timestamp"""
        with self._lock:
            current_physical = int(time.time() * 100)  # Use 10ms granularity for better logical incrementing
            
            if current_physical > self.last_physical:
                self.logical = 0
                self.last_physical = current_physical
            else:
                self.logical += 1
                
            return HLCTimestamp(self.logical, self.last_physical, self.node_id)
    
    def update(self, other_timestamp: HLCTimestamp) -> HLCTimestamp:
        """Update HLC based on received timestamp"""
        with self._lock:
            current_physical = int(time.time() * 100)  # 10ms granularity
            
            # Update last_physical to max of current, last, and received
            max_physical = max(current_physical, self.last_physical, other_timestamp.physical)
            
            if max_physical == current_physical and current_physical > self.last_physical:
                self.logical = 0
            elif max_physical == self.last_physical and self.last_physical > other_timestamp.physical:
                self.logical += 1
            elif max_physical == other_timestamp.physical and other_timestamp.physical > self.last_physical:
                self.logical = other_timestamp.logical + 1
            else:  # max_physical == other_timestamp.physical == self.last_physical
                self.logical = max(self.logical, other_timestamp.logical) + 1
                
            self.last_physical = max_physical
            return HLCTimestamp(self.logical, self.last_physical, self.node_id)
    
    def compare(self, other_timestamp: HLCTimestamp) -> int:
        """Compare HLC timestamps"""
        with self._lock:
            if self.last_physical < other_timestamp.physical:
                return -1
            elif self.last_physical > other_timestamp.physical:
                return 1
            else:  # same physical time
                if self.logical < other_timestamp.logical:
                    return -1
                elif self.logical > other_timestamp.logical:
                    return 1
                else:
                    return 0


class CRDTValue(ABC):
    """Abstract base class for CRDT values"""
    
    @abstractmethod
    def merge(self, other: 'CRDTValue') -> 'CRDTValue':
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
    def from_dict(cls, data: Dict[str, Any]) -> 'CRDTValue':
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
            'value': self.value,
            'timestamp': self.timestamp.__dict__ if hasattr(self.timestamp, '__dict__') else self.timestamp,
            'node_id': self.node_id,
            'deleted': self.deleted
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LWWElement':
        timestamp = data['timestamp']
        if isinstance(timestamp, dict) and 'logical' in timestamp:
            timestamp = HLCTimestamp(**timestamp)
        return cls(
            value=data['value'],
            timestamp=timestamp,
            node_id=data['node_id'],
            deleted=data.get('deleted', False)
        )


class LWWElementSet(CRDTValue):
    """Last-Writer-Wins Element Set CRDT"""
    
    def __init__(self, clock: Clock, node_id: str):
        self.clock = clock
        self.node_id = node_id
        self.elements: Dict[Any, LWWElement] = {}
        self._lock = threading.RLock()
    
    def add(self, value: Any) -> None:
        """Add element to set"""
        with self._lock:
            timestamp = self.clock.tick()
            element = LWWElement(value, timestamp, self.node_id, deleted=False)
            self.elements[value] = element
    
    def remove(self, value: Any) -> None:
        """Remove element from set"""
        with self._lock:
            timestamp = self.clock.tick()
            if value in self.elements:
                # Update existing element
                self.elements[value].deleted = True
                self.elements[value].timestamp = timestamp
                self.elements[value].node_id = self.node_id
            else:
                # Create tombstone
                element = LWWElement(value, timestamp, self.node_id, deleted=True)
                self.elements[value] = element
    
    def contains(self, value: Any) -> bool:
        """Check if element is in set"""
        with self._lock:
            element = self.elements.get(value)
            return element is not None and not element.deleted
    
    def value(self) -> set:
        """Get the current value as a set of all non-deleted elements"""
        with self._lock:
            return {element.value for element in self.elements.values() if not element.deleted}
    
    def get_value(self) -> Any:
        """Get the current value (for single-value LWW register)"""
        with self._lock:
            if not self.elements:
                return None
            
            # Find the most recent non-deleted element
            latest_element = None
            for element in self.elements.values():
                if not element.deleted:
                    if latest_element is None or self._compare_timestamps(element.timestamp, latest_element.timestamp) > 0:
                        latest_element = element
            
            return latest_element.value if latest_element else None
    
    def set_value(self, value: Any) -> None:
        """Set value (for single-value LWW register)"""
        with self._lock:
            timestamp = self.clock.tick()
            # Use a fixed key for single-value register
            element = LWWElement(value, timestamp, self.node_id, deleted=False)
            self.elements['__value__'] = element
    
    def _compare_timestamps(self, ts1: Any, ts2: Any) -> int:
        """Compare timestamps using the appropriate method"""
        if hasattr(ts1, '__dict__') and hasattr(ts2, '__dict__'):  # HLC timestamps
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
            return self.clock.compare(ts1) if hasattr(self.clock, 'compare') else 0
        else:
            # Fallback to simple comparison
            return -1 if ts1 < ts2 else (1 if ts1 > ts2 else 0)
    
    def merge(self, other: 'LWWElementSet') -> 'LWWElementSet':
        """Merge with another LWW set"""
        result = LWWElementSet(self.clock, self.node_id)
        
        with self._lock:
            # Start with all elements from self
            result.elements = self.elements.copy()
            
            # Merge elements from other
            for value, other_element in other.elements.items():
                if value not in result.elements:
                    result.elements[value] = other_element
                else:
                    # Keep the element with later timestamp
                    self_element = result.elements[value]
                    if self._compare_timestamps(other_element.timestamp, self_element.timestamp) > 0:
                        result.elements[value] = other_element
                    elif self._compare_timestamps(other_element.timestamp, self_element.timestamp) == 0:
                        # Same timestamp, use node_id as tiebreaker
                        if other_element.node_id > self_element.node_id:
                            result.elements[value] = other_element
        
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        with self._lock:
            return {
                'type': 'lww_element_set',
                'node_id': self.node_id,
                'elements': {str(k): v.to_dict() for k, v in self.elements.items()}
            }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], clock: Clock, node_id: str) -> 'LWWElementSet':
        """Deserialize from dictionary"""
        result = cls(clock, node_id)
        
        for key, element_data in data.get('elements', {}).items():
            # Try to convert key back to original type
            try:
                # Try common types
                if key.isdigit():
                    actual_key = int(key)
                elif key.replace('.', '').isdigit():
                    actual_key = float(key)
                else:
                    actual_key = key
            except:
                actual_key = key
                
            result.elements[actual_key] = LWWElement.from_dict(element_data)
        
        return result


class ORSet(CRDTValue):
    """Observed-Remove Set CRDT"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.added: Dict[Any, Set[str]] = defaultdict(set)  # element -> set of unique tags
        self.removed: Dict[Any, Set[str]] = defaultdict(set)  # element -> set of unique tags
        self._lock = threading.RLock()
    
    def add(self, value: Any) -> None:
        """Add element to set"""
        with self._lock:
            # Generate unique tag
            tag = f"{self.node_id}:{uuid.uuid4()}"
            self.added[value].add(tag)
    
    def remove(self, value: Any) -> None:
        """Remove element from set"""
        with self._lock:
            # Remove all currently added tags
            if value in self.added:
                for tag in self.added[value]:
                    self.removed[value].add(tag)
    
    def contains(self, value: Any) -> bool:
        """Check if element is in set"""
        with self._lock:
            added_tags = self.added.get(value, set())
            removed_tags = self.removed.get(value, set())
            return len(added_tags - removed_tags) > 0
    
    def values(self) -> Set[Any]:
        """Get all values in the set"""
        with self._lock:
            result = set()
            for value in self.added:
                if self.contains(value):
                    result.add(value)
            return result
    
    def merge(self, other: 'ORSet') -> 'ORSet':
        """Merge with another OR set"""
        result = ORSet(self.node_id)
        
        with self._lock:
            # Merge added sets
            all_values = set(self.added.keys()) | set(other.added.keys())
            for value in all_values:
                result.added[value] = self.added.get(value, set()) | other.added.get(value, set())
            
            # Merge removed sets
            all_values = set(self.removed.keys()) | set(other.removed.keys())
            for value in all_values:
                result.removed[value] = self.removed.get(value, set()) | other.removed.get(value, set())
        
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        with self._lock:
            return {
                'type': 'or_set',
                'node_id': self.node_id,
                'added': {str(k): list(v) for k, v in self.added.items()},
                'removed': {str(k): list(v) for k, v in self.removed.items()}
            }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ORSet':
        """Deserialize from dictionary"""
        result = cls(data['node_id'])
        
        for key, tags in data.get('added', {}).items():
            result.added[key] = set(tags)
        
        for key, tags in data.get('removed', {}).items():
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
    
    def merge(self, other: 'CRDTCounter') -> 'CRDTCounter':
        """Merge with another counter"""
        result = CRDTCounter(self.node_id)
        
        with self._lock:
            # Merge increments
            all_nodes = set(self.increments.keys()) | set(other.increments.keys())
            for node in all_nodes:
                result.increments[node] = max(
                    self.increments.get(node, 0),
                    other.increments.get(node, 0)
                )
            
            # Merge decrements
            all_nodes = set(self.decrements.keys()) | set(other.decrements.keys())
            for node in all_nodes:
                result.decrements[node] = max(
                    self.decrements.get(node, 0),
                    other.decrements.get(node, 0)
                )
        
        return result
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        with self._lock:
            return {
                'type': 'crdt_counter',
                'node_id': self.node_id,
                'increments': dict(self.increments),
                'decrements': dict(self.decrements)
            }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CRDTCounter':
        """Deserialize from dictionary"""
        result = cls(data['node_id'])
        result.increments = defaultdict(int, data.get('increments', {}))
        result.decrements = defaultdict(int, data.get('decrements', {}))
        return result


def create_crdt_value(value_type: str, node_id: str, clock: Optional[Clock] = None, 
                     initial_data: Optional[Dict[str, Any]] = None) -> CRDTValue:
    """Factory function to create CRDT values"""
    if value_type == 'lww_register' or value_type == 'lww_set':
        if clock is None:
            clock = HybridLogicalClock(node_id)
        if initial_data:
            return LWWElementSet.from_dict(initial_data, clock, node_id)
        return LWWElementSet(clock, node_id)
    
    elif value_type == 'or_set':
        if initial_data:
            return ORSet.from_dict(initial_data)
        return ORSet(node_id)
    
    elif value_type == 'counter':
        if initial_data:
            return CRDTCounter.from_dict(initial_data)
        return CRDTCounter(node_id)
    
    else:
        raise ValueError(f"Unknown CRDT type: {value_type}")


def create_crdt(crdt_type: str, value: Any = None, node_id: str = "default", 
                timestamp: Optional[float] = None) -> CRDTValue:
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
        'lww': 'lww_register',
        'lww_set': 'lww_set', 
        'orset': 'or_set',
        'or_set': 'or_set',
        'counter': 'counter'
    }
    
    actual_type = type_mapping.get(crdt_type, crdt_type)
    
    if actual_type in ['lww_register', 'lww_set']:
        clock = HybridLogicalClock(node_id)
        if timestamp:
            # Create HLC timestamp from provided timestamp
            if isinstance(timestamp, (int, float)):
                hlc_timestamp = HLCTimestamp(logical=0, physical=int(timestamp * 1000000))
                # Update the clock with this timestamp
                clock.update(hlc_timestamp)
            elif hasattr(timestamp, 'logical') and hasattr(timestamp, 'physical'):
                # It's already an HLCTimestamp
                clock.update(timestamp)
        
        crdt = LWWElementSet(clock, node_id)
        if value is not None:
            crdt.add(value)
        return crdt
        
    elif actual_type == 'or_set':
        crdt = ORSet(node_id)
        if value is not None:
            if isinstance(value, (list, set)):
                for item in value:
                    crdt.add(item)
            else:
                crdt.add(value)
        return crdt
        
    elif actual_type == 'counter':
        crdt = CRDTCounter(node_id)
        if value is not None and isinstance(value, (int, float)):
            crdt.increment(value)
        return crdt
        
    else:
        raise ValueError(f"Unknown CRDT type: {crdt_type}")
