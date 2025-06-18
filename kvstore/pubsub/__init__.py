"""
High-performance Pub/Sub system for HyperKV
Supports channels, pattern matching, and scalable message delivery
"""

import asyncio
import time
import json
import weakref
from typing import Dict, Set, List, Optional, Any, Callable, AsyncIterator
from collections import defaultdict, deque
from dataclasses import dataclass
import threading
import logging
import fnmatch

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """Pub/Sub message"""
    channel: str
    data: Any
    timestamp: float
    message_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'channel': self.channel,
            'data': self.data,
            'timestamp': self.timestamp,
            'message_id': self.message_id
        }


@dataclass
class Subscription:
    """Subscription information"""
    pattern: str
    callback: Optional[Callable[[Message], None]] = None
    queue: Optional[asyncio.Queue] = None
    created_at: float = 0.0
    
    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()


class Channel:
    """Individual pub/sub channel"""
    
    def __init__(self, name: str, max_buffer_size: int = 1000):
        self.name = name
        self.max_buffer_size = max_buffer_size
        self.subscribers: Set[str] = set()
        self.message_buffer: deque = deque(maxlen=max_buffer_size)
        self.stats = {
            'messages_published': 0,
            'total_subscribers': 0,
            'created_at': time.time()
        }
        self._lock = threading.RLock()
    
    def add_subscriber(self, subscriber_id: str):
        """Add subscriber to channel"""
        with self._lock:
            if subscriber_id not in self.subscribers:
                self.subscribers.add(subscriber_id)
                self.stats['total_subscribers'] += 1
    
    def remove_subscriber(self, subscriber_id: str):
        """Remove subscriber from channel"""
        with self._lock:
            self.subscribers.discard(subscriber_id)
    
    def publish_message(self, message: Message):
        """Publish message to channel"""
        with self._lock:
            self.message_buffer.append(message)
            self.stats['messages_published'] += 1
    
    def get_subscriber_count(self) -> int:
        """Get current subscriber count"""
        with self._lock:
            return len(self.subscribers)
    
    def get_recent_messages(self, count: int = 10) -> List[Message]:
        """Get recent messages from buffer"""
        with self._lock:
            return list(self.message_buffer)[-count:]


class PubSubManager:
    """High-performance Pub/Sub manager"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        
        # Configuration
        self.max_channels = self.config.get('max_channels', 100000)
        self.max_subscribers_per_channel = self.config.get('max_subscribers_per_channel', 1000)
        self.message_buffer_size = self.config.get('message_buffer_size', 1000)
        
        # Storage
        self.channels: Dict[str, Channel] = {}
        self.subscribers: Dict[str, Subscription] = {}
        self.pattern_subscribers: Dict[str, Set[str]] = defaultdict(set)
        
        # Message queues for async subscribers
        self.subscriber_queues: Dict[str, asyncio.Queue] = {}
        
        # Stats
        self.stats = {
            'total_channels': 0,
            'total_subscribers': 0,
            'total_messages': 0,
            'pattern_subscriptions': 0
        }
        
        # Threading
        self._lock = threading.RLock()
        self._message_id_counter = 0
        
        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
    
    def start(self):
        """Start the pub/sub manager"""
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("PubSub manager started")
    
    def stop(self):
        """Stop the pub/sub manager"""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
        logger.info("PubSub manager stopped")
    
    async def _cleanup_loop(self):
        """Background cleanup task"""
        while self._running:
            try:
                await self._cleanup_empty_channels()
                await asyncio.sleep(60)  # Cleanup every minute
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(10)
    
    async def _cleanup_empty_channels(self):
        """Remove empty channels and dead subscribers"""
        with self._lock:
            # Find empty channels
            empty_channels = []
            for channel_name, channel in self.channels.items():
                if channel.get_subscriber_count() == 0:
                    # Keep channel alive for a bit in case of rapid re-subscription
                    if time.time() - channel.stats['created_at'] > 300:  # 5 minutes
                        empty_channels.append(channel_name)
            
            # Remove empty channels
            for channel_name in empty_channels:
                del self.channels[channel_name]
                self.stats['total_channels'] -= 1
            
            if empty_channels:
                logger.debug(f"Cleaned up {len(empty_channels)} empty channels")
    
    def _generate_message_id(self) -> str:
        """Generate unique message ID"""
        with self._lock:
            self._message_id_counter += 1
            return f"{int(time.time())}-{self._message_id_counter}"
    
    def _get_or_create_channel(self, channel_name: str) -> Channel:
        """Get or create a channel"""
        with self._lock:
            if channel_name not in self.channels:
                if len(self.channels) >= self.max_channels:
                    raise RuntimeError(f"Maximum number of channels ({self.max_channels}) reached")
                
                self.channels[channel_name] = Channel(channel_name, self.message_buffer_size)
                self.stats['total_channels'] += 1
            
            return self.channels[channel_name]
    
    def subscribe(self, subscriber_id: str, channel: str, 
                 callback: Optional[Callable[[Message], None]] = None) -> bool:
        """Subscribe to a channel"""
        try:
            with self._lock:
                # Check subscriber limits
                if len(self.subscribers) >= self.max_subscribers_per_channel * self.max_channels:
                    return False
                
                # Get or create channel
                channel_obj = self._get_or_create_channel(channel)
                
                # Check channel subscriber limit
                if channel_obj.get_subscriber_count() >= self.max_subscribers_per_channel:
                    return False
                
                # Create subscription
                subscription = Subscription(pattern=channel, callback=callback)
                
                # Create async queue if no callback provided
                if callback is None:
                    queue = asyncio.Queue(maxsize=self.message_buffer_size)
                    subscription.queue = queue
                    self.subscriber_queues[subscriber_id] = queue
                
                # Store subscription
                self.subscribers[subscriber_id] = subscription
                
                # Add to channel
                channel_obj.add_subscriber(subscriber_id)
                
                self.stats['total_subscribers'] += 1
                
                logger.debug(f"Subscriber {subscriber_id} subscribed to channel {channel}")
                return True
                
        except Exception as e:
            logger.error(f"Error subscribing {subscriber_id} to {channel}: {e}")
            return False
    
    def psubscribe(self, subscriber_id: str, pattern: str,
                  callback: Optional[Callable[[Message], None]] = None) -> bool:
        """Subscribe to channels matching a pattern"""
        try:
            with self._lock:
                # Create subscription
                subscription = Subscription(pattern=pattern, callback=callback)
                
                # Create async queue if no callback provided
                if callback is None:
                    queue = asyncio.Queue(maxsize=self.message_buffer_size)
                    subscription.queue = queue
                    self.subscriber_queues[subscriber_id] = queue
                
                # Store subscription
                self.subscribers[subscriber_id] = subscription
                
                # Add to pattern subscribers
                self.pattern_subscribers[pattern].add(subscriber_id)
                
                self.stats['total_subscribers'] += 1
                self.stats['pattern_subscriptions'] += 1
                
                logger.debug(f"Subscriber {subscriber_id} subscribed to pattern {pattern}")
                return True
                
        except Exception as e:
            logger.error(f"Error pattern subscribing {subscriber_id} to {pattern}: {e}")
            return False
    
    def unsubscribe(self, subscriber_id: str, channel: Optional[str] = None) -> bool:
        """Unsubscribe from channel(s)"""
        try:
            with self._lock:
                if subscriber_id not in self.subscribers:
                    return False
                
                subscription = self.subscribers[subscriber_id]
                
                if channel is None or subscription.pattern == channel:
                    # Remove from all relevant places
                    if subscription.pattern in self.channels:
                        self.channels[subscription.pattern].remove_subscriber(subscriber_id)
                    
                    # Remove from pattern subscribers
                    for pattern, subs in self.pattern_subscribers.items():
                        subs.discard(subscriber_id)
                    
                    # Clean up empty pattern sets
                    empty_patterns = [p for p, subs in self.pattern_subscribers.items() if len(subs) == 0]
                    for pattern in empty_patterns:
                        del self.pattern_subscribers[pattern]
                        self.stats['pattern_subscriptions'] -= 1
                    
                    # Remove subscription
                    del self.subscribers[subscriber_id]
                    
                    # Remove queue
                    if subscriber_id in self.subscriber_queues:
                        del self.subscriber_queues[subscriber_id]
                    
                    self.stats['total_subscribers'] -= 1
                    
                    logger.debug(f"Subscriber {subscriber_id} unsubscribed")
                    return True
                
                return False
                
        except Exception as e:
            logger.error(f"Error unsubscribing {subscriber_id}: {e}")
            return False
    
    async def publish(self, channel: str, data: Any) -> int:
        """Publish message to channel"""
        try:
            # Create message
            message = Message(
                channel=channel,
                data=data,
                timestamp=time.time(),
                message_id=self._generate_message_id()
            )
            
            subscribers_notified = 0
            
            with self._lock:
                # Direct channel subscribers
                if channel in self.channels:
                    channel_obj = self.channels[channel]
                    channel_obj.publish_message(message)
                    
                    for subscriber_id in channel_obj.subscribers.copy():
                        if await self._notify_subscriber(subscriber_id, message):
                            subscribers_notified += 1
                
                # Pattern subscribers
                for pattern, subscriber_ids in self.pattern_subscribers.items():
                    if fnmatch.fnmatch(channel, pattern):
                        for subscriber_id in subscriber_ids.copy():
                            if await self._notify_subscriber(subscriber_id, message):
                                subscribers_notified += 1
                
                self.stats['total_messages'] += 1
            
            logger.debug(f"Published message to {channel}, notified {subscribers_notified} subscribers")
            return subscribers_notified
            
        except Exception as e:
            logger.error(f"Error publishing to {channel}: {e}")
            return 0
    
    async def _notify_subscriber(self, subscriber_id: str, message: Message) -> bool:
        """Notify a single subscriber"""
        try:
            if subscriber_id not in self.subscribers:
                return False
            
            subscription = self.subscribers[subscriber_id]
            
            # Callback-based notification
            if subscription.callback:
                try:
                    subscription.callback(message)
                    return True
                except Exception as e:
                    logger.error(f"Error in subscriber callback {subscriber_id}: {e}")
                    return False
            
            # Queue-based notification
            elif subscription.queue:
                try:
                    subscription.queue.put_nowait(message)
                    return True
                except asyncio.QueueFull:
                    logger.warning(f"Queue full for subscriber {subscriber_id}")
                    return False
            
            return False
            
        except Exception as e:
            logger.error(f"Error notifying subscriber {subscriber_id}: {e}")
            return False
    
    async def get_message(self, subscriber_id: str, timeout: Optional[float] = None) -> Optional[Message]:
        """Get next message for subscriber (queue-based)"""
        if subscriber_id not in self.subscriber_queues:
            return None
        
        queue = self.subscriber_queues[subscriber_id]
        
        try:
            if timeout is None:
                return await queue.get()
            else:
                return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Error getting message for {subscriber_id}: {e}")
            return None
    
    async def get_messages(self, subscriber_id: str) -> AsyncIterator[Message]:
        """Get message stream for subscriber"""
        if subscriber_id not in self.subscriber_queues:
            return
        
        queue = self.subscriber_queues[subscriber_id]
        
        try:
            while self._running:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield message
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error in message stream for {subscriber_id}: {e}")
                    break
        except Exception as e:
            logger.error(f"Error in message stream for {subscriber_id}: {e}")
    
    def get_channels(self) -> List[str]:
        """Get list of active channels"""
        with self._lock:
            return list(self.channels.keys())
    
    def get_channel_info(self, channel: str) -> Optional[Dict[str, Any]]:
        """Get information about a channel"""
        with self._lock:
            if channel not in self.channels:
                return None
            
            channel_obj = self.channels[channel]
            return {
                'name': channel_obj.name,
                'subscribers': channel_obj.get_subscriber_count(),
                'messages_published': channel_obj.stats['messages_published'],
                'created_at': channel_obj.stats['created_at'],
                'recent_messages': len(channel_obj.message_buffer)
            }
    
    def get_subscriber_info(self, subscriber_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a subscriber"""
        with self._lock:
            if subscriber_id not in self.subscribers:
                return None
            
            subscription = self.subscribers[subscriber_id]
            queue_size = 0
            if subscriber_id in self.subscriber_queues:
                queue_size = self.subscriber_queues[subscriber_id].qsize()
            
            return {
                'subscriber_id': subscriber_id,
                'pattern': subscription.pattern,
                'created_at': subscription.created_at,
                'has_callback': subscription.callback is not None,
                'queue_size': queue_size
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pub/sub statistics"""
        with self._lock:
            return {
                **self.stats,
                'active_channels': len(self.channels),
                'active_subscribers': len(self.subscribers),
                'active_queues': len(self.subscriber_queues),
                'memory_usage': self._estimate_memory_usage()
            }
    
    def _estimate_memory_usage(self) -> int:
        """Estimate memory usage in bytes"""
        # Rough estimation
        memory = 0
        
        # Channels
        for channel in self.channels.values():
            memory += len(channel.message_buffer) * 1024  # Rough message size
        
        # Queues
        for queue in self.subscriber_queues.values():
            memory += queue.qsize() * 1024
        
        return memory
    
    # Admin operations
    
    def kill_subscriber(self, subscriber_id: str) -> bool:
        """Force remove a subscriber"""
        return self.unsubscribe(subscriber_id)
    
    def clear_channel(self, channel: str) -> bool:
        """Clear all messages from a channel"""
        with self._lock:
            if channel in self.channels:
                self.channels[channel].message_buffer.clear()
                return True
            return False
    
    def get_recent_messages(self, channel: str, count: int = 10) -> List[Dict[str, Any]]:
        """Get recent messages from a channel"""
        with self._lock:
            if channel not in self.channels:
                return []
            
            messages = self.channels[channel].get_recent_messages(count)
            return [msg.to_dict() for msg in messages]
