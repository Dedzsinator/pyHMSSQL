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
    """
    Send JSON data over a socket.
    """
    sock.send(json.dumps(data, cls=JSONEncoder).encode())


def receive_data(sock):
    """
    Receive data from a socket.
    """
    try:
        data = b""
        while True:
            part = sock.recv(4096)
            if not part:
                break  # Connection closed by client
            data += part
            if len(part) < 4096:
                break  # End of message

        if not data:
            logging.warning("Received empty data from client")
            return None

        # Debug the raw received data
        logging.debug("Raw received data: %s", data)

        # Try to decode as JSON
        try:
            decoded = data.decode("utf-8")
            logging.debug("Decoded string: %s", decoded)
            parsed_data = json.loads(decoded)

            # Verify the parsed data is properly formatted
            if not isinstance(parsed_data, dict):
                logging.error("Parsed data is not a dictionary: %s", parsed_data)
                return {
                    "type": "error",
                    "error": "Invalid data format, expected JSON object",
                }

            # Ensure type field exists
            if "type" not in parsed_data:
                logging.error("Missing 'type' field in request: %s", parsed_data)
                # Set a default type to avoid None errors
                parsed_data["type"] = "unknown"

            return parsed_data
        except json.JSONDecodeError as e:
            logging.error("JSON decode error: %s", str(e))
            logging.error(
                "Attempted to parse: %s", data.decode('utf-8', errors='replace')
                )
            return {"type": "error", "error": f"Invalid JSON format: {str(e)}"}
        except RuntimeError as e:
            logging.error("Error parsing received data: %s", str(e))
            return {"type": "error", "error": f"Data parsing error: {str(e)}"}
    except RuntimeError as e:
        logging.error("Socket receive error: %s", str(e))
        return {"type": "error", "error": f"Communication error: {str(e)}"}
