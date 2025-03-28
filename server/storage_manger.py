import os
import struct
import threading
import logging
import json
import time
from typing import Any, Dict, List, Tuple, Optional, Union, BinaryIO

logger = logging.getLogger('storage_manager')

class Record:
    """Represents a record with fixed-size binary encoding"""
    def __init__(self, record_id: int, data: Dict[str, Any]):
        self.record_id = record_id
        self.data = data
        self.is_deleted = False
    
    def encode(self, schema: Dict[str, Dict]) -> bytes:
        """Encode record as bytes based on schema"""
        result = struct.pack('!I?', self.record_id, self.is_deleted)
        
        # Pack each field according to its type
        for field_name, field_info in schema.items():
            field_type = field_info['type']
            value = self.data.get(field_name)
            
            if field_type == 'int':
                result += struct.pack('!i', value if value is not None else 0)
            elif field_type == 'float':
                result += struct.pack('!f', value if value is not None else 0.0)
            elif field_type == 'bool':
                result += struct.pack('!?', value if value is not None else False)
            elif field_type.startswith('varchar'):
                max_len = int(field_type.split('(')[1][:-1])
                if value is None:
                    value = ''
                encoded_str = value.encode('utf-8')[:max_len]
                result += struct.pack(f'!H{len(encoded_str)}s', len(encoded_str), encoded_str)
            # Add more types as needed
        
        return result
    
    @classmethod
    def decode(cls, data: bytes, schema: Dict[str, Dict]) -> 'Record':
        """Decode record from bytes based on schema"""
        record_id, is_deleted = struct.unpack('!I?', data[:5])
        offset = 5
        
        record_data = {}
        for field_name, field_info in schema.items():
            field_type = field_info['type']
            
            if field_type == 'int':
                value = struct.unpack('!i', data[offset:offset+4])[0]
                offset += 4
                record_data[field_name] = value
            elif field_type == 'float':
                value = struct.unpack('!f', data[offset:offset+4])[0]
                offset += 4
                record_data[field_name] = value
            elif field_type == 'bool':
                value = struct.unpack('!?', data[offset:offset+1])[0]
                offset += 1
                record_data[field_name] = value
            elif field_type.startswith('varchar'):
                str_len = struct.unpack('!H', data[offset:offset+2])[0]
                offset += 2
                value = struct.unpack(f'!{str_len}s', data[offset:offset+str_len])[0].decode('utf-8')
                offset += str_len
                record_data[field_name] = value
            # Add more types as needed
        
        record = cls(record_id, record_data)
        record.is_deleted = is_deleted
        return record
    
class StorageManager:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.tables_dir = os.path.join(data_dir, 'tables')
        os.makedirs(self.tables_dir, exist_ok=True)
        
        # Cache of open file handles
        self.file_handles = {}
        self.file_locks = {}
        self.record_locks = {}
        self.record_versions = {}
        
        # Schema cache
        self.schemas = {}
        
        # Free space management
        self.free_lists = {}
        
        # Record cache
        self.record_cache = {}
        self.cache_size = 1000  # Maximum records to cache per table
        self.cache_hits = 0
        self.cache_misses = 0
        
        self.lock = threading.RLock()
        
        # Load existing schemas
        self._load_schemas()
        
        logger.info(f"Storage manager initialized at {data_dir}")
        
    def _get_record_lock(self, table_name: str, offset: int) -> threading.RLock:
        """Get a lock for a specific record"""
        if table_name not in self.record_locks:
            self.record_locks[table_name] = {}
        
        if offset not in self.record_locks[table_name]:
            self.record_locks[table_name][offset] = threading.RLock()
        
        return self.record_locks[table_name][offset]
    
    def _get_record_version(self, table_name: str, offset: int) -> int:
        """Get the current version of a record"""
        if table_name not in self.record_versions:
            self.record_versions[table_name] = {}
        
        if offset not in self.record_versions[table_name]:
            self.record_versions[table_name][offset] = 1
        
        return self.record_versions[table_name][offset]
    
    def _increment_record_version(self, table_name: str, offset: int) -> int:
        """Increment and return the version of a record"""
        if table_name not in self.record_versions:
            self.record_versions[table_name] = {}
        
        if offset not in self.record_versions[table_name]:
            self.record_versions[table_name][offset] = 1
        else:
            self.record_versions[table_name][offset] += 1
        
        return self.record_versions[table_name][offset]
    
    def _cache_record(self, table_name: str, offset: int, record: Record) -> None:
        """Add a record to the cache"""
        if table_name not in self.record_cache:
            self.record_cache[table_name] = {}
            
        # Implement LRU eviction if cache is full
        if len(self.record_cache[table_name]) >= self.cache_size:
            # Remove oldest accessed record (simple approach)
            oldest_offset = next(iter(self.record_cache[table_name]))
            del self.record_cache[table_name][oldest_offset]
            
        self.record_cache[table_name][offset] = {
            'record': record,
            'timestamp': time.time()
        }
        
    def optimistic_update(self, table_name: str, offset: int, data: Dict[str, Any], expected_version: int) -> bool:
        """Update a record using optimistic concurrency control"""
        try:
            schema = self.schemas.get(table_name)
            if not schema:
                raise ValueError(f"Table {table_name} not found")
            
            # Get the current version
            current_version = self._get_record_version(table_name, offset)
            
            # Check if the record hasn't been modified since read
            if current_version != expected_version:
                logger.warning(f"Optimistic concurrency control failure: expected version {expected_version}, got {current_version}")
                return False
            
            # Get record lock for this specific update
            record_lock = self._get_record_lock(table_name, offset)
            
            with record_lock:
                # Perform the update
                if not self.update_record(table_name, offset, data):
                    return False
                
                # Increment the version number
                self._increment_record_version(table_name, offset)
                
                return True
        
        except Exception as e:
            logger.error(f"Error in optimistic update for {table_name} at offset {offset}: {e}")
            return False
        
    def compact_table(self, table_name: str) -> bool:
        """Compact a table by removing deleted records"""
        with self.lock:
            try:
                schema = self.schemas.get(table_name)
                if not schema:
                    raise ValueError(f"Table {table_name} not found")
                
                record_size = schema['_record_size']
                header_size = schema['_header_size']
                
                file_handle, file_lock = self._get_file_handle(table_name)
                
                with file_lock:
                    # Create a temporary file for the compacted data
                    temp_file_path = os.path.join(self.tables_dir, f"{table_name}_temp.dat")
                    with open(temp_file_path, 'wb') as temp_file:
                        # Write header with placeholder values
                        temp_file.write(struct.pack('!II', 0, 0))
                        
                        # Read header from original file
                        file_handle.seek(0)
                        record_count, next_record_id = struct.unpack('!II', file_handle.read(8))
                        
                        # Track new offsets for records
                        new_offsets = {}
                        valid_record_count = 0
                        
                        # Read all records and copy non-deleted ones
                        file_handle.seek(header_size)
                        current_offset = header_size
                        new_offset = header_size
                        
                        while True:
                            data = file_handle.read(record_size)
                            if not data or len(data) < record_size:
                                break
                            
                            record = Record.decode(data, schema)
                            
                            if not record.is_deleted:
                                # Write to new file
                                temp_file.write(record.encode(schema))
                                
                                # Track the new offset
                                new_offsets[current_offset] = new_offset
                                
                                new_offset += record_size
                                valid_record_count += 1
                            
                            current_offset += record_size
                        
                        # Update header in temp file
                        temp_file.seek(0)
                        temp_file.write(struct.pack('!II', valid_record_count, next_record_id))
                    
                    # Close the original file
                    file_handle.close()
                    del self.file_handles[table_name]
                    
                    # Replace original with compacted file
                    original_path = os.path.join(self.tables_dir, f"{table_name}.dat")
                    os.replace(temp_file_path, original_path)
                    
                    # Update cache with new offsets
                    if table_name in self.record_cache:
                        updated_cache = {}
                        for old_offset, cache_entry in self.record_cache[table_name].items():
                            if old_offset in new_offsets:
                                updated_cache[new_offsets[old_offset]] = cache_entry
                        self.record_cache[table_name] = updated_cache
                    
                    # Clear free list since we've compacted
                    self.free_lists[table_name] = []
                    self._save_free_list(table_name)
                    
                    # Reopen the file handle for future operations
                    self.file_handles[table_name] = open(original_path, 'r+b')
                    
                    # Update space usage metrics
                    old_size = current_offset
                    new_size = new_offset
                    space_saved = old_size - new_size
                    
                    logger.info(f"Table {table_name} compacted: {record_count - valid_record_count} deleted records removed, {space_saved} bytes saved")
                    return True
                    
            except Exception as e:
                logger.error(f"Failed to compact table {table_name}: {e}")
                
                # Clean up any temporary files
                temp_file_path = os.path.join(self.tables_dir, f"{table_name}_temp.dat")
                if os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                    except Exception:
                        pass
                        
                # Ensure file handle is reopened if it was closed
                if table_name not in self.file_handles:
                    try:
                        original_path = os.path.join(self.tables_dir, f"{table_name}.dat")
                        if os.path.exists(original_path):
                            self.file_handles[table_name] = open(original_path, 'r+b')
                    except Exception:
                        pass
                        
                return False

    def _get_cached_record(self, table_name: str, offset: int) -> Optional[Record]:
        """Get a record from cache if available"""
        if table_name in self.record_cache and offset in self.record_cache[table_name]:
            # Update access timestamp
            self.record_cache[table_name][offset]['timestamp'] = time.time()
            self.cache_hits += 1
            return self.record_cache[table_name][offset]['record']
        
        self.cache_misses += 1
        return None
    
    def _invalidate_cache(self, table_name: str, offset: int = None) -> None:
        """Invalidate cache entries for a table or specific record"""
        if table_name in self.record_cache:
            if offset is not None:
                if offset in self.record_cache[table_name]:
                    del self.record_cache[table_name][offset]
            else:
                del self.record_cache[table_name]
    
    def _load_schemas(self) -> None:
        """Load all table schemas from disk"""
        schema_dir = os.path.join(self.data_dir, 'schemas')
        os.makedirs(schema_dir, exist_ok=True)
        
        for filename in os.listdir(schema_dir):
            if filename.endswith('.json'):
                table_name = filename[:-5]
                schema_path = os.path.join(schema_dir, filename)
                try:
                    with open(schema_path, 'r') as f:
                        self.schemas[table_name] = json.load(f)
                    
                    # Load free list
                    free_list_path = os.path.join(self.data_dir, 'free_lists', f"{table_name}.json")
                    if os.path.exists(free_list_path):
                        with open(free_list_path, 'r') as f:
                            self.free_lists[table_name] = json.load(f)
                    else:
                        self.free_lists[table_name] = []
                
                except Exception as e:
                    logger.error(f"Failed to load schema for {table_name}: {e}")
    
    def create_table(self, table_name: str, schema: Dict[str, Dict]) -> bool:
        """Create a new table with the given schema"""
        with self.lock:
            schema_dir = os.path.join(self.data_dir, 'schemas')
            os.makedirs(schema_dir, exist_ok=True)
            
            free_list_dir = os.path.join(self.data_dir, 'free_lists')
            os.makedirs(free_list_dir, exist_ok=True)
            
            # Check if table already exists
            schema_path = os.path.join(schema_dir, f"{table_name}.json")
            if os.path.exists(schema_path):
                logger.warning(f"Table {table_name} already exists")
                return False
            
            # Compute fixed record size
            record_size = self._compute_record_size(schema)
            schema['_record_size'] = record_size
            schema['_header_size'] = 8  # Table header size
            
            # Save the schema
            try:
                with open(schema_path, 'w') as f:
                    json.dump(schema, f, indent=2)
                
                self.schemas[table_name] = schema
                
                # Create the data file
                table_file = os.path.join(self.tables_dir, f"{table_name}.dat")
                with open(table_file, 'wb') as f:
                    # Write header: record count and next record ID
                    f.write(struct.pack('!II', 0, 1))
                
                # Initialize free list
                self.free_lists[table_name] = []
                with open(os.path.join(free_list_dir, f"{table_name}.json"), 'w') as f:
                    json.dump(self.free_lists[table_name], f)
                
                return True
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                return False
    
    def _compute_record_size(self, schema: Dict[str, Dict]) -> int:
        """Compute the fixed size for records based on schema"""
        size = 5  # record_id (4) + is_deleted flag (1)
        
        for field_name, field_info in schema.items():
            if field_name.startswith('_'):  # Skip metadata fields
                continue
                
            field_type = field_info['type']
            if field_type == 'int':
                size += 4
            elif field_type == 'float':
                size += 4
            elif field_type == 'bool':
                size += 1
            elif field_type.startswith('varchar'):
                max_len = int(field_type.split('(')[1][:-1])
                size += 2 + max_len  # Length prefix (2) + max string length
            # Add more types as needed
        
        return size
    
    def _get_file_handle(self, table_name: str) -> Tuple[BinaryIO, threading.RLock]:
        """Get a file handle for the table, opening it if necessary"""
        if table_name not in self.file_handles:
            table_file = os.path.join(self.tables_dir, f"{table_name}.dat")
            if not os.path.exists(table_file):
                raise FileNotFoundError(f"Table file not found: {table_file}")
            
            self.file_handles[table_name] = open(table_file, 'r+b')
            self.file_locks[table_name] = threading.RLock()
        
        return self.file_handles[table_name], self.file_locks[table_name]
    
    def read_record(self, table_name: str, offset: int) -> Optional[Record]:
        """Read a record at the specified offset"""
        try:
            schema = self.schemas.get(table_name)
            if not schema:
                raise ValueError(f"Table {table_name} not found")
            
            # Try to get from cache first
            cached_record = self._get_cached_record(table_name, offset)
            if cached_record is not None:
                # Skip deleted records
                if cached_record.is_deleted:
                    return None
                return cached_record
            
            record_size = schema['_record_size']
            
            file_handle, file_lock = self._get_file_handle(table_name)
            
            with file_lock:
                file_handle.seek(offset)
                data = file_handle.read(record_size)
                
                if not data or len(data) < record_size:
                    return None
                
                record = Record.decode(data, schema)
                
                # Skip deleted records
                if record.is_deleted:
                    return None
                
                # Add to cache
                self._cache_record(table_name, offset, record)
                
                return record
        
        except Exception as e:
            logger.error(f"Error reading record from {table_name} at offset {offset}: {e}")
            return None

    def update_record(self, table_name: str, offset: int, data: Dict[str, Any]) -> bool:
        """Update a record at the specified offset"""
        try:
            schema = self.schemas.get(table_name)
            if not schema:
                raise ValueError(f"Table {table_name} not found")
            
            file_handle, file_lock = self._get_file_handle(table_name)
            
            with file_lock:
                # Read the existing record
                file_handle.seek(offset)
                record_data = file_handle.read(schema['_record_size'])
                
                if not record_data:
                    return False
                
                record = Record.decode(record_data, schema)
                
                if record.is_deleted:
                    return False
                
                # Update fields
                record.data.update(data)
                
                # Encode and write back
                encoded_record = record.encode(schema)
                file_handle.seek(offset)
                file_handle.write(encoded_record)
                file_handle.flush()
                
                # Update cache
                self._cache_record(table_name, offset, record)
                
                return True
        
        except Exception as e:
            logger.error(f"Error updating record in {table_name} at offset {offset}: {e}")
            return False
    
    def delete_record(self, table_name: str, offset: int) -> bool:
        """Mark a record as deleted at the specified offset"""
        try:
            schema = self.schemas.get(table_name)
            if not schema:
                raise ValueError(f"Table {table_name} not found")
            
            file_handle, file_lock = self._get_file_handle(table_name)
            
            with file_lock:
                # Read the existing record
                file_handle.seek(offset)
                record_data = file_handle.read(schema['_record_size'])
                
                if not record_data:
                    return False
                
                record = Record.decode(record_data, schema)
                
                # Mark as deleted
                record.is_deleted = True
                
                # Write the updated record back
                file_handle.seek(offset)
                file_handle.write(record.encode(schema))
                
                # Update header (decrement record count)
                file_handle.seek(0)
                record_count, next_record_id = struct.unpack('!II', file_handle.read(8))
                record_count -= 1
                file_handle.seek(0)
                file_handle.write(struct.pack('!II', record_count, next_record_id))
                
                # Add to free list for space reuse
                self.free_lists[table_name].append(offset)
                self._save_free_list(table_name)
                
                file_handle.flush()
                
                return True
        
        except Exception as e:
            logger.error(f"Error deleting record from {table_name} at offset {offset}: {e}")
            return False
    
    def _save_free_list(self, table_name: str) -> None:
        """Save the free list for a table"""
        free_list_dir = os.path.join(self.data_dir, 'free_lists')
        os.makedirs(free_list_dir, exist_ok=True)
        
        try:
            with open(os.path.join(free_list_dir, f"{table_name}.json"), 'w') as f:
                json.dump(self.free_lists[table_name], f)
        except Exception as e:
            logger.error(f"Error saving free list for {table_name}: {e}")
    
    def drop_table(self, table_name: str) -> bool:
        """Delete a table and its associated files"""
        with self.lock:
            try:
                schema_dir = os.path.join(self.data_dir, 'schemas')
                free_list_dir = os.path.join(self.data_dir, 'free_lists')
                
                # Close file handle if open
                if table_name in self.file_handles:
                    self.file_handles[table_name].close()
                    del self.file_handles[table_name]
                    del self.file_locks[table_name]
                
                # Delete schema file
                schema_path = os.path.join(schema_dir, f"{table_name}.json")
                if os.path.exists(schema_path):
                    os.remove(schema_path)
                
                # Delete free list file
                free_list_path = os.path.join(free_list_dir, f"{table_name}.json")
                if os.path.exists(free_list_path):
                    os.remove(free_list_path)
                
                # Delete table data file
                table_file = os.path.join(self.tables_dir, f"{table_name}.dat")
                if os.path.exists(table_file):
                    os.remove(table_file)
                
                # Remove from schemas cache
                if table_name in self.schemas:
                    del self.schemas[table_name]
                
                # Remove from free lists
                if table_name in self.free_lists:
                    del self.free_lists[table_name]
                
                return True
            
            except Exception as e:
                logger.error(f"Error dropping table {table_name}: {e}")
                return False
            
    def batch_insert(self, table_name: str, records: List[Dict[str, Any]]) -> List[Tuple[int, int]]:
        """Insert multiple records in a batch operation for better performance"""
        results = []
        
        with self.lock:
            try:
                schema = self.schemas.get(table_name)
                if not schema:
                    raise ValueError(f"Table {table_name} not found")
                
                record_size = schema['_record_size']
                header_size = schema['_header_size']
                
                file_handle, file_lock = self._get_file_handle(table_name)
                
                with file_lock:
                    # Read table header
                    file_handle.seek(0)
                    record_count, next_record_id = struct.unpack('!II', file_handle.read(8))
                    
                    free_list = self.free_lists.get(table_name, [])
                    
                    # Process all records
                    for data in records:
                        record_id = next_record_id
                        next_record_id += 1
                        
                        # Try to reuse space from deleted records
                        if free_list:
                            offset = free_list.pop(0)
                        else:
                            # Append to end of file
                            offset = header_size + record_count * record_size
                        
                        # Create and write the record
                        record = Record(record_id, data)
                        encoded_record = record.encode(schema)
                        
                        file_handle.seek(offset)
                        file_handle.write(encoded_record)
                        
                        # Add to cache
                        self._cache_record(table_name, offset, record)
                        
                        # Store result
                        results.append((record_id, offset))
                        record_count += 1
                    
                    # Update free list if needed
                    if table_name in self.free_lists:
                        self.free_lists[table_name] = free_list
                        self._save_free_list(table_name)
                    
                    # Update header
                    file_handle.seek(0)
                    file_handle.write(struct.pack('!II', record_count, next_record_id))
                    file_handle.flush()
                    
                    return results
            
            except Exception as e:
                logger.error(f"Error batch inserting records into {table_name}: {e}")
                raise