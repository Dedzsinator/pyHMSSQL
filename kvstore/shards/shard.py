    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair"""
        start_time = time.time()
        
        if not self.running:
            return
        
        with self.lock:
            self.data[key] = value
            self.memory_usage += len(str(key)) + len(str(value))
            
            # Record operation latency
            latency = time.time() - start_time
            self.operation_latencies.append(latency)
            
            # Keep only recent latencies
            if len(self.operation_latencies) > 1000:
                self.operation_latencies.popleft()

    def get(self, key: str) -> Any:
        """Get a value by key"""
        start_time = time.time()
        
        if not self.running:
            return None
        
        with self.lock:
            value = self.data.get(key)
            
            # Record operation latency
            latency = time.time() - start_time
            self.operation_latencies.append(latency)
            
            # Keep only recent latencies
            if len(self.operation_latencies) > 1000:
                self.operation_latencies.popleft()
                
            return value
