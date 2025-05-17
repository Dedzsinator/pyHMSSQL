"""_summary_

Returns:
    _type_: _description_
"""
import json
from bson import ObjectId
import logging


class JSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle MongoDB ObjectId.
    """

    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return super().default(o)


def get_current_database_or_error(catalog_manager, include_type=True):
    """
    Get the current database or return a standardized error response.

    Args:
        catalog_manager: The catalog manager instance
        include_type: Whether to include 'type' field in error response

    Returns:
        tuple: (db_name, None) if database is selected
                (None, error_dict) if no database is selected
    """
    db_name = catalog_manager.get_current_database()
    if not db_name:
        error = {
            "error": "No database selected. Use 'USE database_name' first.",
            "status": "error",
        }
        if include_type:
            error["type"] = "error"
        return None, error
    return db_name, None


def send_data(sock, data):
    """Send JSON data to a socket with size prefix"""
    # Serialize the data to JSON
    json_data = json.dumps(data).encode('utf-8')
    
    # Send the size of the data as 4 bytes (big-endian) 
    size_bytes = len(json_data).to_bytes(4, byteorder='big')
    sock.sendall(size_bytes)
    
    # Send the actual JSON data
    sock.sendall(json_data)

def receive_data(sock):
    """Receive JSON data from a socket with size prefix"""
    # Read the data size (4 bytes, big-endian)
    size_bytes = sock.recv(4)
    if not size_bytes:
        raise RuntimeError("Connection closed by remote host")
    
    size = int.from_bytes(size_bytes, byteorder='big')
    
    # Read the JSON data according to the size
    data_bytes = bytearray()
    bytes_remaining = size
    
    while bytes_remaining > 0:
        chunk = sock.recv(min(bytes_remaining, 4096))
        if not chunk:
            raise RuntimeError("Connection closed by remote host")
        data_bytes.extend(chunk)
        bytes_remaining -= len(chunk)
    
    # Deserialize the JSON data
    return json.loads(data_bytes.decode('utf-8'))