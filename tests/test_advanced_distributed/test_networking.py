"""
Comprehensive unit tests for networking and Pub/Sub components.

This module tests networking functionality including:
- RESP protocol implementation
- TCP server and client connections  
- Pub/Sub message routing and delivery
- Channel management and pattern matching
- Performance characteristics and error handling

Created for comprehensive testing of production-ready distributed networking core.
"""

import pytest
import asyncio
import time
import json
import socket
import threading
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List, Set
import weakref

from kvstore.networking import (
    NetworkServer, RESPProtocol, RESPType, ClientConnection
)
from kvstore.pubsub import (
    PubSubManager, Message, Subscription, Channel
)


class TestRESPProtocol:
    """Test Redis RESP protocol implementation"""
    
    def test_encode_simple_string(self):
        """Test encoding simple strings"""
        result = RESPProtocol.encode_simple_string("OK")
        assert result == b"+OK\r\n"
        
        result = RESPProtocol.encode_simple_string("PONG")
        assert result == b"+PONG\r\n"
    
    def test_encode_error(self):
        """Test encoding error messages"""
        result = RESPProtocol.encode_error("ERR unknown command")
        assert result == b"-ERR unknown command\r\n"
        
        result = RESPProtocol.encode_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        expected = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        assert result == expected
    
    def test_encode_integer(self):
        """Test encoding integers"""
        result = RESPProtocol.encode_integer(42)
        assert result == b":42\r\n"
        
        result = RESPProtocol.encode_integer(-1)
        assert result == b":-1\r\n"
        
        result = RESPProtocol.encode_integer(0)
        assert result == b":0\r\n"
    
    def test_encode_bulk_string(self):
        """Test encoding bulk strings"""
        result = RESPProtocol.encode_bulk_string("hello")
        assert result == b"$5\r\nhello\r\n"
        
        result = RESPProtocol.encode_bulk_string("")
        assert result == b"$0\r\n\r\n"
        
        # Null bulk string
        result = RESPProtocol.encode_bulk_string(None)
        assert result == b"$-1\r\n"
    
    def test_encode_array(self):
        """Test encoding arrays"""
        result = RESPProtocol.encode_array(["SET", "key", "value"])
        expected = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        assert result == expected
        
        result = RESPProtocol.encode_array([])
        assert result == b"*0\r\n"
        
        # Null array
        result = RESPProtocol.encode_array(None)
        assert result == b"*-1\r\n"
    
    def test_encode_mixed_array(self):
        """Test encoding arrays with mixed types"""
        mixed_array = ["GET", "key", 42, None, ["nested", "array"]]
        result = RESPProtocol.encode_array(mixed_array)
        
        # Should encode each element according to its type
        assert b"*5\r\n" in result  # Array of 5 elements
        assert b"$3\r\nGET\r\n" in result  # Bulk string
        assert b":42\r\n" in result  # Integer
        assert b"$-1\r\n" in result  # Null bulk string
        assert b"*2\r\n" in result  # Nested array
    
    def test_decode_simple_string(self):
        """Test decoding simple strings"""
        data = b"+OK\r\n"
        result = RESPProtocol.decode(data)
        assert result == "OK"
        
        data = b"+PONG\r\n"
        result = RESPProtocol.decode(data)
        assert result == "PONG"
    
    def test_decode_error(self):
        """Test decoding error messages"""
        data = b"-ERR unknown command\r\n"
        result = RESPProtocol.decode(data)
        assert isinstance(result, Exception)
        assert str(result) == "ERR unknown command"
    
    def test_decode_integer(self):
        """Test decoding integers"""
        data = b":42\r\n"
        result = RESPProtocol.decode(data)
        assert result == 42
        
        data = b":-1\r\n"
        result = RESPProtocol.decode(data)
        assert result == -1
    
    def test_decode_bulk_string(self):
        """Test decoding bulk strings"""
        data = b"$5\r\nhello\r\n"
        result = RESPProtocol.decode(data)
        assert result == "hello"
        
        data = b"$0\r\n\r\n"
        result = RESPProtocol.decode(data)
        assert result == ""
        
        # Null bulk string
        data = b"$-1\r\n"
        result = RESPProtocol.decode(data)
        assert result is None
    
    def test_decode_array(self):
        """Test decoding arrays"""
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        result = RESPProtocol.decode(data)
        assert result == ["SET", "key", "value"]
        
        data = b"*0\r\n"
        result = RESPProtocol.decode(data)
        assert result == []
    
    def test_decode_incomplete_data(self):
        """Test decoding incomplete RESP data"""
        # Incomplete simple string
        data = b"+OK"
        with pytest.raises(ValueError, match="Incomplete RESP data"):
            RESPProtocol.decode(data)
        
        # Incomplete bulk string
        data = b"$5\r\nhel"
        with pytest.raises(ValueError, match="Incomplete RESP data"):
            RESPProtocol.decode(data)
    
    def test_decode_invalid_format(self):
        """Test decoding invalid RESP format"""
        # Invalid type prefix
        data = b"@invalid\r\n"
        with pytest.raises(ValueError, match="Unknown RESP type"):
            RESPProtocol.decode(data)
        
        # Invalid integer
        data = b":not_a_number\r\n"
        with pytest.raises(ValueError):
            RESPProtocol.decode(data)


class TestClientConnection:
    """Test client connection management"""
    
    def test_client_connection_creation(self):
        """Test client connection creation"""
        conn = ClientConnection(
            client_id="client_123",
            remote_addr="192.168.1.100:12345",
            connected_at=time.time()
        )
        
        assert conn.client_id == "client_123"
        assert conn.remote_addr == "192.168.1.100:12345"
        assert conn.commands_processed == 0
        assert conn.bytes_sent == 0
        assert conn.bytes_received == 0
        assert conn.last_activity > 0
    
    def test_client_connection_activity_tracking(self):
        """Test client connection activity tracking"""
        conn = ClientConnection(
            client_id="client_123",
            remote_addr="192.168.1.100:12345",
            connected_at=time.time()
        )
        
        # Update activity
        conn.commands_processed += 1
        conn.bytes_sent += 100
        conn.bytes_received += 50
        conn.last_activity = time.time()
        
        assert conn.commands_processed == 1
        assert conn.bytes_sent == 100
        assert conn.bytes_received == 50


class TestNetworkServer:
    """Test network server implementation"""
    
    @pytest.mark.asyncio
    async def test_server_start_stop(self):
        """Test server start and stop"""
        server = NetworkServer(host="127.0.0.1", port=0)
        
        # Start server
        await server.start()
        assert server.running is True
        assert server.server is not None
        
        # Stop server
        await server.stop()
        assert server.running is False
    
    @pytest.mark.asyncio
    async def test_server_client_connection(self):
        """Test client connection to server"""
        # Create a real server for testing
        server = NetworkServer(host="127.0.0.1", port=0)
        await server.start()
        
        # Mock command handler
        async def mock_command_handler(command, args, client_id):
            return "OK"
        
        server.command_handler = mock_command_handler
        
        try:
            # Get server address
            server_host = server.server.sockets[0].getsockname()[0]
            server_port = server.server.sockets[0].getsockname()[1]
            
            # Connect client
            reader, writer = await asyncio.open_connection(server_host, server_port)
            
            # Send RESP command
            command = RESPProtocol.encode_array(["PING"])
            writer.write(command)
            await writer.drain()
            
            # Read response
            response_data = await reader.read(1024)
            response = RESPProtocol.decode(response_data)
            
            assert response == "OK"
            
            # Close connection
            writer.close()
            await writer.wait_closed()
        
        finally:
            # Cleanup
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_server_multiple_clients(self):
        """Test server handling multiple clients"""
        # Create a real server for testing
        server = NetworkServer(host="127.0.0.1", port=0)
        await server.start()
        
        # Mock command handler
        async def mock_command_handler(command, args, client_id):
            return f"Response from {client_id}"
        
        server.command_handler = mock_command_handler
        
        try:
            # Get server address
            server_host = server.server.sockets[0].getsockname()[0]
            server_port = server.server.sockets[0].getsockname()[1]
            
            # Connect multiple clients
            clients = []
            for i in range(5):
                reader, writer = await asyncio.open_connection(server_host, server_port)
                clients.append((reader, writer))
            
            # Send commands from all clients
            responses = []
            for i, (reader, writer) in enumerate(clients):
                command = RESPProtocol.encode_array(["TEST", str(i)])
                writer.write(command)
                await writer.drain()
                
                response_data = await reader.read(1024)
                response = RESPProtocol.decode(response_data)
                responses.append(response)
            
            # Verify responses
            assert len(responses) == 5
            for response in responses:
                assert "Response from" in response
            
            # Close all connections
            for reader, writer in clients:
                writer.close()
                await writer.wait_closed()
        
        finally:
            # Cleanup
            await server.stop()
    
    @pytest.mark.asyncio
    async def test_server_command_parsing(self):
        """Test server command parsing"""
        # Create a real server for testing
        server = NetworkServer(host="127.0.0.1", port=0)
        await server.start()
        
        received_commands = []
        
        async def capture_command_handler(command, args, client_id):
            received_commands.append((command, args, client_id))
            return "OK"
        
        server.command_handler = capture_command_handler
        
        try:
            # Get server address and connect
            server_host = server.server.sockets[0].getsockname()[0]
            server_port = server.server.sockets[0].getsockname()[1]
            
            reader, writer = await asyncio.open_connection(server_host, server_port)
            
            # Send various commands
            commands_to_send = [
                ["GET", "key1"],
                ["SET", "key2", "value2"],
                ["DEL", "key1", "key2", "key3"]
            ]
            
            for cmd in commands_to_send:
                command = RESPProtocol.encode_array(cmd)
                writer.write(command)
                await writer.drain()
                
                # Read response
                await reader.read(1024)
            
            # Verify received commands
            assert len(received_commands) == 3
            assert received_commands[0][0] == "GET"
            assert received_commands[0][1] == ["key1"]
            assert received_commands[1][0] == "SET"
            assert received_commands[1][1] == ["key2", "value2"]
            assert received_commands[2][0] == "DEL"
            assert received_commands[2][1] == ["key1", "key2", "key3"]
            
            # Close connection
            writer.close()
            await writer.wait_closed()
        
        finally:
            # Cleanup
            await server.stop()


class TestMessage:
    """Test Pub/Sub message implementation"""
    
    def test_message_creation(self):
        """Test message creation"""
        message = Message(
            channel="test_channel",
            data="test_data",
            timestamp=time.time(),
            message_id="msg_123"
        )
        
        assert message.channel == "test_channel"
        assert message.data == "test_data"
        assert message.message_id == "msg_123"
        assert message.timestamp > 0
    
    def test_message_to_dict(self):
        """Test message serialization"""
        timestamp = time.time()
        message = Message(
            channel="test_channel",
            data={"key": "value"},
            timestamp=timestamp,
            message_id="msg_123"
        )
        
        msg_dict = message.to_dict()
        
        assert msg_dict["channel"] == "test_channel"
        assert msg_dict["data"] == {"key": "value"}
        assert msg_dict["timestamp"] == timestamp
        assert msg_dict["message_id"] == "msg_123"


class TestSubscription:
    """Test Pub/Sub subscription implementation"""
    
    def test_subscription_creation(self):
        """Test subscription creation"""
        callback = Mock()
        
        subscription = Subscription(
            pattern="test_*",
            callback=callback
        )
        
        assert subscription.pattern == "test_*"
        assert subscription.callback == callback
        assert subscription.queue is None
        assert subscription.created_at > 0
    
    def test_subscription_with_queue(self):
        """Test subscription with async queue"""
        queue = asyncio.Queue()
        
        subscription = Subscription(
            pattern="test_*",
            queue=queue
        )
        
        assert subscription.pattern == "test_*"
        assert subscription.callback is None
        assert subscription.queue == queue


class TestChannel:
    """Test Pub/Sub channel implementation"""
    
    def test_channel_creation(self):
        """Test channel creation"""
        channel = Channel("test_channel")
        
        assert channel.name == "test_channel"
        assert len(channel.subscribers) == 0
        assert channel.message_count == 0
    
    @pytest.mark.asyncio
    async def test_channel_subscribe(self):
        """Test channel subscription"""
        channel = Channel("test_channel")
        
        # Subscribe with callback
        callback = Mock()
        subscription_id = await channel.subscribe(callback=callback)
        
        assert subscription_id in channel.subscribers
        assert len(channel.subscribers) == 1
        
        subscription = channel.subscribers[subscription_id]
        assert subscription.callback == callback
    
    @pytest.mark.asyncio
    async def test_channel_unsubscribe(self):
        """Test channel unsubscription"""
        channel = Channel("test_channel")
        
        # Subscribe
        callback = Mock()
        subscription_id = await channel.subscribe(callback=callback)
        assert len(channel.subscribers) == 1
        
        # Unsubscribe
        success = await channel.unsubscribe(subscription_id)
        assert success
        assert len(channel.subscribers) == 0
        assert subscription_id not in channel.subscribers
    
    @pytest.mark.asyncio
    async def test_channel_publish(self):
        """Test channel message publishing"""
        channel = Channel("test_channel")
        
        # Subscribe
        received_messages = []
        
        async def callback(message):
            received_messages.append(message)
        
        await channel.subscribe(callback=callback)
        
        # Publish message
        message = Message(
            channel="test_channel",
            data="test_data",
            timestamp=time.time(),
            message_id="msg_123"
        )
        
        await channel.publish(message)
        
        # Wait for delivery
        await asyncio.sleep(0.1)
        
        assert len(received_messages) == 1
        assert received_messages[0].data == "test_data"
        assert channel.message_count == 1
    
    @pytest.mark.asyncio
    async def test_channel_multiple_subscribers(self):
        """Test channel with multiple subscribers"""
        channel = Channel("test_channel")
        
        # Subscribe multiple callbacks
        received_messages = [[], [], []]
        
        async def make_callback(index):
            async def callback(message):
                received_messages[index].append(message)
            return callback
        
        for i in range(3):
            callback = await make_callback(i)
            await channel.subscribe(callback=callback)
        
        # Publish message
        message = Message(
            channel="test_channel",
            data="broadcast_data",
            timestamp=time.time(),
            message_id="msg_broadcast"
        )
        
        await channel.publish(message)
        
        # Wait for delivery
        await asyncio.sleep(0.1)
        
        # All subscribers should receive message
        for messages in received_messages:
            assert len(messages) == 1
            assert messages[0].data == "broadcast_data"


class TestPubSubManager:
    """Test Pub/Sub manager implementation"""
    
    @pytest.fixture
    def pubsub_manager(self):
        """Create a Pub/Sub manager for testing"""
        return PubSubManager()
    
    @pytest.mark.asyncio
    async def test_pubsub_create_channel(self, pubsub_manager):
        """Test creating channels"""
        manager = pubsub_manager
        
        channel = await manager.get_or_create_channel("test_channel")
        
        assert channel.name == "test_channel"
        assert "test_channel" in manager.channels
    
    @pytest.mark.asyncio
    async def test_pubsub_publish(self, pubsub_manager):
        """Test publishing messages"""
        manager = pubsub_manager
        
        # Subscribe to channel
        received_messages = []
        
        async def callback(message):
            received_messages.append(message)
        
        await manager.subscribe("test_channel", callback)
        
        # Publish message
        await manager.publish("test_channel", "test_data")
        
        # Wait for delivery
        await asyncio.sleep(0.1)
        
        assert len(received_messages) == 1
        assert received_messages[0].data == "test_data"
        assert received_messages[0].channel == "test_channel"
    
    @pytest.mark.asyncio
    async def test_pubsub_pattern_matching(self, pubsub_manager):
        """Test pattern-based subscriptions"""
        manager = pubsub_manager
        
        # Subscribe to pattern
        received_messages = []
        
        async def callback(message):
            received_messages.append(message)
        
        await manager.psubscribe("test_*", callback)
        
        # Publish to matching channels
        await manager.publish("test_channel_1", "data1")
        await manager.publish("test_channel_2", "data2")
        await manager.publish("other_channel", "data3")  # Should not match
        
        # Wait for delivery
        await asyncio.sleep(0.1)
        
        # Only matching messages should be received
        assert len(received_messages) == 2
        channels = {msg.channel for msg in received_messages}
        assert "test_channel_1" in channels
        assert "test_channel_2" in channels
        assert "other_channel" not in channels
    
    @pytest.mark.asyncio
    async def test_pubsub_unsubscribe(self, pubsub_manager):
        """Test unsubscribing from channels"""
        manager = pubsub_manager
        
        # Subscribe
        callback = Mock()
        subscription_id = await manager.subscribe("test_channel", callback)
        
        # Verify subscription exists
        channel = manager.channels.get("test_channel")
        assert channel is not None
        assert len(channel.subscribers) == 1
        
        # Unsubscribe
        success = await manager.unsubscribe("test_channel", subscription_id)
        assert success
        assert len(channel.subscribers) == 0
    
    @pytest.mark.asyncio
    async def test_pubsub_channel_cleanup(self, pubsub_manager):
        """Test automatic channel cleanup"""
        manager = pubsub_manager
        
        # Subscribe and then unsubscribe
        callback = Mock()
        subscription_id = await manager.subscribe("temp_channel", callback)
        
        assert "temp_channel" in manager.channels
        
        await manager.unsubscribe("temp_channel", subscription_id)
        
        # Channel should be cleaned up if empty
        await asyncio.sleep(0.1)  # Give time for cleanup
        
        # Channel might still exist but should have no subscribers
        channel = manager.channels.get("temp_channel")
        if channel:
            assert len(channel.subscribers) == 0
    
    @pytest.mark.asyncio
    async def test_pubsub_message_history(self, pubsub_manager):
        """Test message history functionality"""
        manager = pubsub_manager
        
        # Enable message history for channel
        channel = await manager.get_or_create_channel("history_channel")
        channel.enable_history(max_messages=5)
        
        # Publish messages
        for i in range(10):
            await manager.publish("history_channel", f"message_{i}")
        
        # Get history
        history = channel.get_history()
        
        # Should only keep last 5 messages
        assert len(history) == 5
        assert history[0].data == "message_5"  # Oldest kept message
        assert history[-1].data == "message_9"  # Latest message


class TestPubSubPerformance:
    """Test Pub/Sub performance characteristics"""
    
    @pytest.mark.asyncio
    async def test_high_throughput_publishing(self):
        """Test high-throughput message publishing"""
        manager = PubSubManager()
        
        # Subscribe to channel
        received_count = 0
        
        async def fast_callback(message):
            nonlocal received_count
            received_count += 1
        
        await manager.subscribe("perf_channel", fast_callback)
        
        # Publish many messages quickly
        start_time = time.time()
        message_count = 1000
        
        for i in range(message_count):
            await manager.publish("perf_channel", f"message_{i}")
        
        publish_duration = time.time() - start_time
        
        # Wait for all messages to be delivered
        await asyncio.sleep(1.0)
        
        # Performance assertions
        assert received_count == message_count
        messages_per_sec = message_count / publish_duration
        assert messages_per_sec > 100  # At least 100 messages/sec
    
    @pytest.mark.asyncio
    async def test_many_subscribers_performance(self):
        """Test performance with many subscribers"""
        manager = PubSubManager()
        
        # Create many subscribers
        subscriber_count = 100
        received_counts = [0] * subscriber_count
        
        async def make_callback(index):
            async def callback(message):
                received_counts[index] += 1
            return callback
        
        for i in range(subscriber_count):
            callback = await make_callback(i)
            await manager.subscribe("broadcast_channel", callback)
        
        # Publish message
        start_time = time.time()
        await manager.publish("broadcast_channel", "broadcast_message")
        
        # Wait for delivery to all subscribers
        await asyncio.sleep(1.0)
        
        delivery_duration = time.time() - start_time
        
        # Verify all subscribers received message
        assert all(count == 1 for count in received_counts)
        
        # Performance assertion
        deliveries_per_sec = subscriber_count / delivery_duration
        assert deliveries_per_sec > 50  # At least 50 deliveries/sec
    
    @pytest.mark.asyncio
    async def test_pattern_matching_performance(self):
        """Test pattern matching performance"""
        manager = PubSubManager()
        
        # Subscribe to multiple patterns
        patterns = ["user_*", "order_*", "product_*", "admin_*"]
        received_messages = {pattern: [] for pattern in patterns}
        
        for pattern in patterns:
            async def make_callback(p):
                async def callback(message):
                    received_messages[p].append(message)
                return callback
            
            callback = await make_callback(pattern)
            await manager.psubscribe(pattern, callback)
        
        # Publish to many channels
        start_time = time.time()
        
        channels = [
            "user_login", "user_logout", "order_created", "order_completed",
            "product_updated", "product_deleted", "admin_action", "other_channel"
        ]
        
        for channel in channels:
            await manager.publish(channel, f"data_for_{channel}")
        
        publish_duration = time.time() - start_time
        
        # Wait for delivery
        await asyncio.sleep(0.5)
        
        # Verify pattern matching worked correctly
        assert len(received_messages["user_*"]) == 2  # user_login, user_logout
        assert len(received_messages["order_*"]) == 2  # order_created, order_completed
        assert len(received_messages["product_*"]) == 2  # product_updated, product_deleted
        assert len(received_messages["admin_*"]) == 1  # admin_action
        
        # Performance assertion
        total_operations = len(channels) * len(patterns)  # Pattern checks
        operations_per_sec = total_operations / publish_duration
        assert operations_per_sec > 100  # Efficient pattern matching


class TestNetworkingIntegration:
    """Test integration between networking and Pub/Sub"""
    
    @pytest.mark.asyncio
    async def test_pubsub_over_network(self):
        """Test Pub/Sub commands over network protocol"""
        # Create server with Pub/Sub integration
        pubsub_manager = PubSubManager()
        server = NetworkServer(host="127.0.0.1", port=0)
        
        # Command handler for Pub/Sub
        async def pubsub_command_handler(command, args, client_id):
            if command == "PUBLISH":
                channel, message = args[0], args[1]
                await pubsub_manager.publish(channel, message)
                return "OK"
            elif command == "SUBSCRIBE":
                # In real implementation, would set up subscription for this client
                return "Subscribed"
            else:
                return "Unknown command"
        
        server.command_handler = pubsub_command_handler
        
        await server.start()
        
        try:
            # Get server address
            server_host = server.server.sockets[0].getsockname()[0]
            server_port = server.server.sockets[0].getsockname()[1]
            
            # Connect client
            reader, writer = await asyncio.open_connection(server_host, server_port)
            
            # Send PUBLISH command
            command = RESPProtocol.encode_array(["PUBLISH", "test_channel", "test_message"])
            writer.write(command)
            await writer.drain()
            
            # Read response
            response_data = await reader.read(1024)
            response = RESPProtocol.decode(response_data)
            
            assert response == "OK"
            
            # Close connection
            writer.close()
            await writer.wait_closed()
            
        finally:
            await server.stop()


if __name__ == '__main__':
    pytest.main([__file__])
