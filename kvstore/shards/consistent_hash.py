class ConsistentHashRing:
    """Consistent hashing ring for distributed data placement"""

    def __init__(self, virtual_nodes: int = 150, num_virtual_nodes: Optional[int] = None):
        # Support both parameter names for backward compatibility
        if num_virtual_nodes is not None:
            virtual_nodes = num_virtual_nodes
        
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.nodes: Set[str] = set()
