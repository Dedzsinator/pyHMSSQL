import json
from bson import ObjectId

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
    Receive JSON data from a socket.
    Handles incomplete or invalid data gracefully.
    """
    try:
        data = sock.recv(1024).decode()
        if not data:
            return None  # No data received
        return json.loads(data)
    except json.JSONDecodeError:
        return None  # Invalid JSON data
    except Exception as e:
        print(f"Error receiving data: {e}")
        return None