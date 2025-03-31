import logging

class ViewManager:
    """Handles view operations"""
    
    def __init__(self, catalog_manager):
        self.catalog_manager = catalog_manager
    
    def execute_view_operation(self, plan):
        """Execute view operations"""
        plan_type = plan.get("type")
        
        if plan_type == "CREATE_VIEW":
            return self.execute_create_view(plan)
        elif plan_type == "DROP_VIEW":
            return self.execute_drop_view(plan)
        else:
            return {"error": f"Unsupported view operation: {plan_type}", "status": "error"}
    
    def execute_create_view(self, plan):
        """Execute CREATE VIEW operation"""
        return self.catalog_manager.create_view(plan["view_name"], plan["query"])
    
    def execute_drop_view(self, plan):
        """Execute DROP VIEW operation"""
        return self.catalog_manager.drop_view(plan["view_name"])