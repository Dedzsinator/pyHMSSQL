"""
Test script focused on the original B+ tree implementation
"""
import random
import logging
import sys
from bptree import BPlusTree

# Configure logging for better diagnostics
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_original_tree(num_insertions=100, order=5):
    """Test the original tree with controlled insertions"""
    logger.info(f"Testing original B+ tree with {num_insertions} insertions, order {order}")
    
    # Create tree
    tree = BPlusTree(order=order, name="test_tree")
    
    # Try inserting sequential keys first
    logger.info("Inserting sequential keys...")
    missing_keys = []
    
    for i in range(num_insertions):
        try:
            tree.insert(i, f"value_{i}")
            if i % 10 == 0:
                logger.debug(f"  Inserted key: {i}")
        except RuntimeError as e:
            logger.error(f"Error at key {i}: {e}")
            return
    
    # Verify all keys were inserted
    for i in range(num_insertions):
        value = tree.search(i)
        if value != f"value_{i}":
            missing_keys.append(i)
            if len(missing_keys) <= 5:  # Only print the first few missing keys
                logger.error(f"FAILED: Key {i} returned {value} instead of value_{i}")
    
    if missing_keys:
        logger.error(f"Found {len(missing_keys)} missing keys out of {num_insertions}")
        logger.error(f"Missing key pattern: {missing_keys[:20]}...")
        return
    
    logger.info("All sequential keys verified successfully!")
    
    # Try random keys
    logger.info("\nInserting random keys...")
    random_keys = [random.randint(num_insertions, num_insertions*2) for _ in range(num_insertions)]
    
    for i, key in enumerate(random_keys):
        try:
            tree.insert(key, f"random_{key}")
            if i % 10 == 0:
                logger.debug(f"  Inserted random key: {key}")
        except RuntimeError as e:
            logger.error(f"Error at random key {key}: {e}")
            return
    
    # Verify random keys
    missing_random = []
    for key in random_keys:
        value = tree.search(key)
        if value != f"random_{key}":
            missing_random.append(key)
            if len(missing_random) <= 5:  # Only print the first few missing keys
                logger.error(f"FAILED: Random key {key} returned {value} instead of random_{key}")
    
    if missing_random:
        logger.error(f"Found {len(missing_random)} missing random keys out of {len(random_keys)}")
        logger.error(f"Missing random keys pattern: {missing_random[:20]}...")
        return
    
    logger.info("All random keys verified successfully!")
    
    # Test range query
    start = 5
    end = 15
    logger.info(f"Testing range query from {start} to {end}...")
    range_results = tree.range_query(start, end)
    range_keys = [item[0] for item in range_results]
    
    # Check if all keys in range are returned
    missing_range = []
    for i in range(start, end + 1):
        if i not in range_keys:
            missing_range.append(i)
            if len(missing_range) <= 5:  # Only print the first few missing keys
                logger.error(f"FAILED: Key {i} missing from range query results")
    
    if missing_range:
        logger.error(f"Found {len(missing_range)} missing keys from range query out of {end-start+1}")
        logger.error(f"Missing range keys: {missing_range}")
        return
    
    logger.info(f"Range query successful, returned {len(range_results)} items")
    
    logger.info("All tests passed successfully!")
    return tree

if __name__ == "__main__":
    # Allow customizing test parameters from command line
    num_keys = 100
    tree_order = 5
    
    if len(sys.argv) > 1:
        num_keys = int(sys.argv[1])
    if len(sys.argv) > 2:
        tree_order = int(sys.argv[2])
        
    test_original_tree(num_keys, tree_order)