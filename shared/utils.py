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
        data = b''
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
        logging.debug(f"Raw received data: {data}")
        
        # Try to decode as JSON
        try:
            decoded = data.decode('utf-8')
            logging.debug(f"Decoded string: {decoded}")
            parsed_data = json.loads(decoded)
            
            # Verify the parsed data is properly formatted
            if not isinstance(parsed_data, dict):
                logging.error(f"Parsed data is not a dictionary: {parsed_data}")
                return {"type": "error", "error": "Invalid data format, expected JSON object"}
                
            # Ensure type field exists
            if 'type' not in parsed_data:
                logging.error(f"Missing 'type' field in request: {parsed_data}")
                # Set a default type to avoid None errors
                parsed_data['type'] = "unknown"
                
            return parsed_data
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {str(e)}")
            logging.error(f"Attempted to parse: {data.decode('utf-8', errors='replace')}")
            return {"type": "error", "error": f"Invalid JSON format: {str(e)}"}
        except Exception as e:
            logging.error(f"Error parsing received data: {str(e)}")
            return {"type": "error", "error": f"Data parsing error: {str(e)}"}
    except Exception as e:
        logging.error(f"Socket receive error: {str(e)}")
        return {"type": "error", "error": f"Communication error: {str(e)}"}