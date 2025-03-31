class TransactionManager:
    """Handles database transaction operations"""
    
    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
        self.transaction_active = False
    
    def execute_transaction_operation(self, operation_type):
        """Execute transaction operations"""
        if operation_type == "BEGIN_TRANSACTION":
            return self.execute_begin_transaction()
        elif operation_type == "COMMIT":
            return self.execute_commit_transaction()
        elif operation_type == "ROLLBACK":
            return self.execute_rollback_transaction()
        else:
            return {"error": f"Unsupported transaction operation: {operation_type}", "status": "error"}
    
    def execute_begin_transaction(self):
        """Begin a new transaction"""
        result = self.catalog_manager.begin_transaction()
        self.transaction_active = True
        return {"message": result}
    
    def execute_commit_transaction(self):
        """Commit the current transaction"""
        if not self.transaction_active:
            return {"error": "No active transaction to commit", "status": "error"}
            
        result = self.catalog_manager.commit_transaction()
        self.transaction_active = False
        return {"message": result}
    
    def execute_rollback_transaction(self):
        """Rollback the current transaction"""
        if not self.transaction_active:
            return {"error": "No active transaction to rollback", "status": "error"}
            
        result = self.catalog_manager.rollback_transaction()
        self.transaction_active = False
        return {"message": result}
