"""
High-performance networking layer for HyperKV
Supports Redis RESP protocol and TCP server implementation
"""

import asyncio
import logging
import time
import json
from typing import Dict, List, Any, Optional, Callable, Union
import socket
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class RESPType(Enum):
    """RESP data types"""
    SIMPLE_STRING = "+"
    ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"


@dataclass
class ClientConnection:
    """Client connection information"""
    client_id: str
    remote_addr: str
    connected_at: float
    commands_processed: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    last_activity: float = 0.0
    
    def __post_init__(self):
        if self.last_activity == 0.0:
            self.last_activity = time.time()


class RESPProtocol:
    """Redis RESP protocol implementation"""
    
    @staticmethod
    def encode_simple_string(data: str) -> bytes:
        """Encode simple string: +OK\\r\\n"""
        return f"+{data}\\r\\n".encode('utf-8')
    
    @staticmethod
    def encode_error(error: str) -> bytes:
        """Encode error: -Error message\\r\\n"""
        return f"-{error}\\r\\n".encode('utf-8')
    
    @staticmethod
    def encode_integer(num: int) -> bytes:
        """Encode integer: :1000\\r\\n"""
        return f":{num}\\r\\n".encode('utf-8')
    
    @staticmethod
    def encode_bulk_string(data: Optional[str]) -> bytes:
        """Encode bulk string: $6\\r\\nfoobar\\r\\n or $-1\\r\\n for null"""
        if data is None:
            return b"$-1\\r\\n"
        
        data_bytes = data.encode('utf-8')
        return f"${len(data_bytes)}\\r\\n".encode('utf-8') + data_bytes + b"\\r\\n"
    
    @staticmethod
    def encode_array(items: Optional[List[Any]]) -> bytes:
        """Encode array: *2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n"""
        if items is None:
            return b"*-1\\r\\n"
        
        result = f"*{len(items)}\\r\\n".encode('utf-8')
        
        for item in items:
            if isinstance(item, str):
                result += RESPProtocol.encode_bulk_string(item)
            elif isinstance(item, int):
                result += RESPProtocol.encode_integer(item)
            elif isinstance(item, list):
                result += RESPProtocol.encode_array(item)
            elif item is None:
                result += RESPProtocol.encode_bulk_string(None)
            else:
                result += RESPProtocol.encode_bulk_string(str(item))
        
        return result
    
    @staticmethod
    def encode_response(data: Any) -> bytes:
        """Encode response based on data type"""
        if isinstance(data, str):
            if data.startswith("ERR ") or data.startswith("ERROR "):
                return RESPProtocol.encode_error(data)
            return RESPProtocol.encode_bulk_string(data)
        elif isinstance(data, int):
            return RESPProtocol.encode_integer(data)
        elif isinstance(data, list):
            return RESPProtocol.encode_array(data)
        elif data is None:
            return RESPProtocol.encode_bulk_string(None)
        elif isinstance(data, bool):
            return RESPProtocol.encode_integer(1 if data else 0)
        else:
            return RESPProtocol.encode_bulk_string(str(data))


class RESPParser:
    """RESP protocol parser"""
    
    def __init__(self):
        self.buffer = b""
    
    def feed(self, data: bytes):
        """Feed data to parser"""
        self.buffer += data
    
    def parse(self) -> List[Any]:
        """Parse complete commands from buffer"""
        commands = []
        
        while self.buffer:
            try:
                command, consumed = self._parse_single()
                if command is None:
                    break  # Need more data
                
                commands.append(command)
                self.buffer = self.buffer[consumed:]
                
            except Exception as e:
                logger.error(f"RESP parsing error: {e}")
                # Skip to next CRLF to recover
                crlf_pos = self.buffer.find(b"\\r\\n")
                if crlf_pos != -1:
                    self.buffer = self.buffer[crlf_pos + 2:]
                else:
                    self.buffer = b""
                break
        
        return commands
    
    def _parse_single(self) -> tuple[Optional[Any], int]:
        """Parse a single RESP element"""
        if len(self.buffer) < 1:
            return None, 0
        
        type_byte = chr(self.buffer[0])
        
        if type_byte == "+":
            return self._parse_simple_string()
        elif type_byte == "-":
            return self._parse_error()
        elif type_byte == ":":
            return self._parse_integer()
        elif type_byte == "$":
            return self._parse_bulk_string()
        elif type_byte == "*":
            return self._parse_array()
        else:
            raise ValueError(f"Unknown RESP type: {type_byte}")
    
    def _parse_simple_string(self) -> tuple[Optional[str], int]:
        """Parse simple string: +OK\\r\\n"""
        crlf_pos = self.buffer.find(b"\\r\\n", 1)
        if crlf_pos == -1:
            return None, 0
        
        data = self.buffer[1:crlf_pos].decode('utf-8')
        return data, crlf_pos + 2
    
    def _parse_error(self) -> tuple[Optional[str], int]:
        """Parse error: -Error message\\r\\n"""
        crlf_pos = self.buffer.find(b"\\r\\n", 1)
        if crlf_pos == -1:
            return None, 0
        
        error = self.buffer[1:crlf_pos].decode('utf-8')
        return f"ERROR {error}", crlf_pos + 2
    
    def _parse_integer(self) -> tuple[Optional[int], int]:
        """Parse integer: :1000\\r\\n"""
        crlf_pos = self.buffer.find(b"\\r\\n", 1)
        if crlf_pos == -1:
            return None, 0
        
        num_str = self.buffer[1:crlf_pos].decode('utf-8')
        return int(num_str), crlf_pos + 2
    
    def _parse_bulk_string(self) -> tuple[Optional[str], int]:
        """Parse bulk string: $6\\r\\nfoobar\\r\\n"""
        first_crlf = self.buffer.find(b"\\r\\n", 1)
        if first_crlf == -1:
            return None, 0
        
        length_str = self.buffer[1:first_crlf].decode('utf-8')
        length = int(length_str)
        
        if length == -1:
            return None, first_crlf + 2
        
        # Check if we have enough data
        total_needed = first_crlf + 2 + length + 2
        if len(self.buffer) < total_needed:
            return None, 0
        
        data_start = first_crlf + 2
        data_end = data_start + length
        data = self.buffer[data_start:data_end].decode('utf-8')
        
        return data, total_needed
    
    def _parse_array(self) -> tuple[Optional[List[Any]], int]:
        """Parse array: *2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n"""
        first_crlf = self.buffer.find(b"\\r\\n", 1)
        if first_crlf == -1:
            return None, 0
        
        count_str = self.buffer[1:first_crlf].decode('utf-8')
        count = int(count_str)
        
        if count == -1:
            return None, first_crlf + 2
        
        # Parse array elements
        elements = []
        consumed = first_crlf + 2
        remaining_buffer = self.buffer[consumed:]
        
        for _ in range(count):
            parser = RESPParser()
            parser.buffer = remaining_buffer
            
            element, element_consumed = parser._parse_single()
            if element is None:
                return None, 0  # Need more data
            
            elements.append(element)
            consumed += element_consumed
            remaining_buffer = remaining_buffer[element_consumed:]
        
        return elements, consumed


class RedisProtocolHandler:
    """Redis protocol command handler"""
    
    def __init__(self, kvstore_server):
        self.kvstore_server = kvstore_server
        
        # Command handlers
        self.commands = {
            # Basic commands
            'GET': self._handle_get,
            'SET': self._handle_set,
            'DEL': self._handle_del,
            'EXISTS': self._handle_exists,
            'MGET': self._handle_mget,
            'MSET': self._handle_mset,
            
            # TTL commands
            'EXPIRE': self._handle_expire,
            'TTL': self._handle_ttl,
            'PERSIST': self._handle_persist,
            
            # Scan commands
            'KEYS': self._handle_keys,
            'SCAN': self._handle_scan,
            'FLUSHDB': self._handle_flushdb,
            
            # Pub/Sub commands
            'SUBSCRIBE': self._handle_subscribe,
            'UNSUBSCRIBE': self._handle_unsubscribe,
            'PUBLISH': self._handle_publish,
            'PSUBSCRIBE': self._handle_psubscribe,
            'PUNSUBSCRIBE': self._handle_punsubscribe,
            
            # Info commands
            'PING': self._handle_ping,
            'INFO': self._handle_info,
            'CLIENT': self._handle_client,
            
            # Admin commands
            'CONFIG': self._handle_config,
            'SHUTDOWN': self._handle_shutdown,
        }
    
    async def handle_command(self, client_id: str, command: List[str]) -> bytes:
        """Handle a Redis command"""
        if not command:
            return RESPProtocol.encode_error("ERR empty command")
        
        cmd_name = command[0].upper()
        
        if cmd_name not in self.commands:
            return RESPProtocol.encode_error(f"ERR unknown command '{cmd_name}'")
        
        try:
            result = await self.commands[cmd_name](client_id, command[1:])
            return RESPProtocol.encode_response(result)
        except Exception as e:
            logger.error(f"Error handling command {cmd_name}: {e}")
            return RESPProtocol.encode_error(f"ERR {str(e)}")
    
    # Basic commands
    
    async def _handle_get(self, client_id: str, args: List[str]) -> Any:
        if len(args) != 1:
            raise ValueError("wrong number of arguments for 'get' command")
        
        key = args[0]
        value = await self.kvstore_server.get(key)
        return value.decode('utf-8') if value else None
    
    async def _handle_set(self, client_id: str, args: List[str]) -> Any:
        if len(args) < 2:
            raise ValueError("wrong number of arguments for 'set' command")
        
        key = args[0]
        value = args[1]
        
        # Parse optional parameters (EX, PX, NX, XX)
        options = {}
        i = 2
        while i < len(args):
            option = args[i].upper()
            if option in ['EX', 'PX'] and i + 1 < len(args):
                options[option] = int(args[i + 1])
                i += 2
            elif option in ['NX', 'XX']:
                options[option] = True
                i += 1
            else:
                i += 1
        
        await self.kvstore_server.set(key, value.encode('utf-8'), **options)
        return "OK"
    
    async def _handle_del(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'del' command")
        
        count = 0
        for key in args:
            if await self.kvstore_server.delete(key):
                count += 1
        
        return count
    
    async def _handle_exists(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'exists' command")
        
        count = 0
        for key in args:
            if await self.kvstore_server.exists(key):
                count += 1
        
        return count
    
    async def _handle_mget(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'mget' command")
        
        results = []
        for key in args:
            value = await self.kvstore_server.get(key)
            results.append(value.decode('utf-8') if value else None)
        
        return results
    
    async def _handle_mset(self, client_id: str, args: List[str]) -> Any:
        if len(args) % 2 != 0:
            raise ValueError("wrong number of arguments for 'mset' command")
        
        for i in range(0, len(args), 2):
            key = args[i]
            value = args[i + 1]
            await self.kvstore_server.set(key, value.encode('utf-8'))
        
        return "OK"
    
    # TTL commands
    
    async def _handle_expire(self, client_id: str, args: List[str]) -> Any:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'expire' command")
        
        key = args[0]
        seconds = int(args[1])
        
        result = await self.kvstore_server.expire(key, seconds)
        return 1 if result else 0
    
    async def _handle_ttl(self, client_id: str, args: List[str]) -> Any:
        if len(args) != 1:
            raise ValueError("wrong number of arguments for 'ttl' command")
        
        key = args[0]
        ttl = await self.kvstore_server.ttl(key)
        return ttl
    
    async def _handle_persist(self, client_id: str, args: List[str]) -> Any:
        if len(args) != 1:
            raise ValueError("wrong number of arguments for 'persist' command")
        
        key = args[0]
        result = await self.kvstore_server.persist(key)
        return 1 if result else 0
    
    # Scan commands
    
    async def _handle_keys(self, client_id: str, args: List[str]) -> Any:
        pattern = args[0] if args else "*"
        keys = await self.kvstore_server.keys(pattern)
        return keys
    
    async def _handle_scan(self, client_id: str, args: List[str]) -> Any:
        cursor = int(args[0]) if args else 0
        
        # Parse options
        match = "*"
        count = 10
        
        i = 1
        while i < len(args):
            if args[i].upper() == "MATCH" and i + 1 < len(args):
                match = args[i + 1]
                i += 2
            elif args[i].upper() == "COUNT" and i + 1 < len(args):
                count = int(args[i + 1])
                i += 2
            else:
                i += 1
        
        next_cursor, keys = await self.kvstore_server.scan(cursor, match, count)
        return [str(next_cursor), keys]
    
    async def _handle_flushdb(self, client_id: str, args: List[str]) -> Any:
        await self.kvstore_server.clear()
        return "OK"
    
    # Pub/Sub commands
    
    async def _handle_subscribe(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'subscribe' command")
        
        results = []
        for channel in args:
            success = await self.kvstore_server.subscribe(client_id, channel)
            results.append(["subscribe", channel, 1 if success else 0])
        
        return results
    
    async def _handle_unsubscribe(self, client_id: str, args: List[str]) -> Any:
        channels = args if args else [None]
        
        results = []
        for channel in channels:
            success = await self.kvstore_server.unsubscribe(client_id, channel)
            results.append(["unsubscribe", channel or "", 0])
        
        return results
    
    async def _handle_publish(self, client_id: str, args: List[str]) -> Any:
        if len(args) != 2:
            raise ValueError("wrong number of arguments for 'publish' command")
        
        channel = args[0]
        message = args[1]
        
        count = await self.kvstore_server.publish(channel, message)
        return count
    
    async def _handle_psubscribe(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'psubscribe' command")
        
        results = []
        for pattern in args:
            success = await self.kvstore_server.psubscribe(client_id, pattern)
            results.append(["psubscribe", pattern, 1 if success else 0])
        
        return results
    
    async def _handle_punsubscribe(self, client_id: str, args: List[str]) -> Any:
        patterns = args if args else [None]
        
        results = []
        for pattern in patterns:
            success = await self.kvstore_server.unsubscribe(client_id, pattern)
            results.append(["punsubscribe", pattern or "", 0])
        
        return results
    
    # Info commands
    
    async def _handle_ping(self, client_id: str, args: List[str]) -> Any:
        return args[0] if args else "PONG"
    
    async def _handle_info(self, client_id: str, args: List[str]) -> Any:
        section = args[0] if args else "default"
        info = await self.kvstore_server.get_info(section)
        
        # Format as Redis INFO format
        lines = []
        for key, value in info.items():
            lines.append(f"{key}:{value}")
        
        return "\\r\\n".join(lines)
    
    async def _handle_client(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'client' command")
        
        subcommand = args[0].upper()
        
        if subcommand == "LIST":
            clients = await self.kvstore_server.get_client_list()
            lines = []
            for client in clients:
                line = f"id={client['client_id']} addr={client['remote_addr']} age={int(time.time() - client['connected_at'])}"
                lines.append(line)
            return "\\n".join(lines)
        
        elif subcommand == "KILL" and len(args) >= 2:
            target_client_id = args[1]
            success = await self.kvstore_server.kill_client(target_client_id)
            return "OK" if success else "ERR no such client"
        
        else:
            raise ValueError(f"unknown CLIENT subcommand '{subcommand}'")
    
    # Admin commands
    
    async def _handle_config(self, client_id: str, args: List[str]) -> Any:
        if not args:
            raise ValueError("wrong number of arguments for 'config' command")
        
        subcommand = args[0].upper()
        
        if subcommand == "GET" and len(args) >= 2:
            parameter = args[1]
            value = await self.kvstore_server.get_config(parameter)
            return [parameter, value] if value is not None else []
        
        elif subcommand == "SET" and len(args) >= 3:
            parameter = args[1]
            value = args[2]
            success = await self.kvstore_server.set_config(parameter, value)
            return "OK" if success else "ERR"
        
        else:
            raise ValueError(f"unknown CONFIG subcommand '{subcommand}'")
    
    async def _handle_shutdown(self, client_id: str, args: List[str]) -> Any:
        await self.kvstore_server.shutdown()
        return "OK"


class TcpServer:
    """High-performance TCP server for HyperKV"""
    
    def __init__(self, host: str, port: int, handler: RedisProtocolHandler, config: Dict[str, Any] = None):
        self.host = host
        self.port = port
        self.handler = handler
        self.config = config or {}
        
        # Server state
        self.server: Optional[asyncio.Server] = None
        self.clients: Dict[str, ClientConnection] = {}
        self.client_parsers: Dict[str, RESPParser] = {}
        self.client_writers: Dict[str, asyncio.StreamWriter] = {}
        
        # Configuration
        self.max_connections = self.config.get('max_connections', 10000)
        self.tcp_nodelay = self.config.get('tcp_nodelay', True)
        self.tcp_keepalive = self.config.get('tcp_keepalive', True)
        
        # Stats
        self.stats = {
            'connections_created': 0,
            'connections_closed': 0,
            'commands_processed': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'errors': 0
        }
        
        self._client_id_counter = 0
        self._running = False
    
    async def start(self):
        """Start the TCP server"""
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=2**16,  # 64KB buffer
            reuse_address=True,
            reuse_port=True
        )
        
        self._running = True
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"HyperKV server started on {addr[0]}:{addr[1]}")
        
        # Start background tasks
        asyncio.create_task(self._cleanup_task())
        
        await self.server.serve_forever()
    
    async def stop(self):
        """Stop the TCP server"""
        self._running = False
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Close all client connections
        for writer in self.client_writers.values():
            writer.close()
            await writer.wait_closed()
        
        logger.info("HyperKV server stopped")
    
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a client connection"""
        client_id = str(self._client_id_counter)
        self._client_id_counter += 1
        
        # Get client address
        peername = writer.get_extra_info('peername')
        remote_addr = f"{peername[0]}:{peername[1]}" if peername else "unknown"
        
        # Configure socket
        sock = writer.get_extra_info('socket')
        if sock:
            if self.tcp_nodelay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self.tcp_keepalive:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # Create client connection
        client = ClientConnection(
            client_id=client_id,
            remote_addr=remote_addr,
            connected_at=time.time()
        )
        
        # Check connection limit
        if len(self.clients) >= self.max_connections:
            await self._send_error(writer, "ERR max number of clients reached")
            writer.close()
            return
        
        # Register client
        self.clients[client_id] = client
        self.client_parsers[client_id] = RESPParser()
        self.client_writers[client_id] = writer
        self.stats['connections_created'] += 1
        
        logger.debug(f"Client {client_id} connected from {remote_addr}")
        
        try:
            # Handle client requests
            async for data in reader:
                if not data:
                    break
                
                client.bytes_received += len(data)
                client.last_activity = time.time()
                self.stats['bytes_received'] += len(data)
                
                # Parse commands
                parser = self.client_parsers[client_id]
                parser.feed(data)
                
                commands = parser.parse()
                
                for command in commands:
                    if isinstance(command, list) and command:
                        try:
                            response = await self.handler.handle_command(client_id, command)
                            await self._send_response(writer, response)
                            
                            client.commands_processed += 1
                            client.bytes_sent += len(response)
                            self.stats['commands_processed'] += 1
                            self.stats['bytes_sent'] += len(response)
                            
                        except Exception as e:
                            logger.error(f"Error handling command from {client_id}: {e}")
                            error_response = RESPProtocol.encode_error(f"ERR {str(e)}")
                            await self._send_response(writer, error_response)
                            self.stats['errors'] += 1
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error handling client {client_id}: {e}")
        finally:
            # Clean up client
            await self._cleanup_client(client_id)
    
    async def _send_response(self, writer: asyncio.StreamWriter, response: bytes):
        """Send response to client"""
        try:
            writer.write(response)
            await writer.drain()
        except Exception as e:
            logger.error(f"Error sending response: {e}")
            raise
    
    async def _send_error(self, writer: asyncio.StreamWriter, error: str):
        """Send error response to client"""
        response = RESPProtocol.encode_error(error)
        await self._send_response(writer, response)
    
    async def _cleanup_client(self, client_id: str):
        """Clean up client connection"""
        if client_id in self.clients:
            del self.clients[client_id]
        
        if client_id in self.client_parsers:
            del self.client_parsers[client_id]
        
        if client_id in self.client_writers:
            writer = self.client_writers[client_id]
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except:
                    pass
            del self.client_writers[client_id]
        
        self.stats['connections_closed'] += 1
        logger.debug(f"Client {client_id} disconnected")
    
    async def _cleanup_task(self):
        """Background cleanup task"""
        while self._running:
            try:
                await asyncio.sleep(30)  # Run every 30 seconds
                
                # Clean up inactive clients (optional timeout)
                timeout = self.config.get('client_timeout')
                if timeout:
                    now = time.time()
                    inactive_clients = []
                    
                    for client_id, client in self.clients.items():
                        if now - client.last_activity > timeout:
                            inactive_clients.append(client_id)
                    
                    for client_id in inactive_clients:
                        await self._cleanup_client(client_id)
                        logger.info(f"Disconnected inactive client {client_id}")
                
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            **self.stats,
            'active_connections': len(self.clients),
            'max_connections': self.max_connections
        }
    
    def get_client_list(self) -> List[Dict[str, Any]]:
        """Get list of connected clients"""
        return [
            {
                'client_id': client.client_id,
                'remote_addr': client.remote_addr,
                'connected_at': client.connected_at,
                'commands_processed': client.commands_processed,
                'bytes_sent': client.bytes_sent,
                'bytes_received': client.bytes_received,
                'last_activity': client.last_activity
            }
            for client in self.clients.values()
        ]
    
    async def kill_client(self, client_id: str) -> bool:
        """Kill a client connection"""
        if client_id in self.clients:
            await self._cleanup_client(client_id)
            return True
        return False
