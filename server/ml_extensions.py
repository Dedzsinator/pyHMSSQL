"""
Machine Learning Extensions for Query Optimization.

This module provides optional ML-based extensions that can improve
cardinality estimation and join order selection through learned models.

Features:
- XGBoost-based cardinality estimation
- Attention/RL-based join order policies
- Feature extraction from query plans
- Model training and inference infrastructure
- Integration with the cost-based optimizer
"""

import logging
import math
import pickle
import os
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import numpy as np
import json

# Optional ML dependencies - fail gracefully if not available
try:
    import xgboost as xgb
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False
    logging.warning("XGBoost not available - ML cardinality estimation disabled")

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    HAS_PYTORCH = True
except ImportError:
    HAS_PYTORCH = False
    logging.warning("PyTorch not available - ML join order policies disabled")


@dataclass
class QueryFeatures:
    """Features extracted from a query for ML models."""
    table_count: int
    join_count: int
    predicate_count: int
    aggregate_count: int
    table_sizes: List[int]
    join_types: List[str]
    predicate_selectivities: List[float]
    column_cardinalities: List[int]
    index_availability: List[bool]
    query_complexity_score: float
    
    def to_vector(self) -> np.ndarray:
        """Convert features to a numerical vector for ML models."""
        # Basic features
        features = [
            self.table_count,
            self.join_count,
            self.predicate_count,
            self.aggregate_count,
            self.query_complexity_score
        ]
        
        # Aggregate table sizes
        if self.table_sizes:
            features.extend([
                sum(self.table_sizes),
                max(self.table_sizes),
                min(self.table_sizes),
                np.mean(self.table_sizes),
                np.std(self.table_sizes)
            ])
        else:
            features.extend([0, 0, 0, 0, 0])
            
        # Join type encoding (binary features)
        join_type_counts = {'INNER': 0, 'LEFT': 0, 'RIGHT': 0, 'FULL': 0}
        for jt in self.join_types:
            if jt in join_type_counts:
                join_type_counts[jt] += 1
        features.extend(list(join_type_counts.values()))
        
        # Selectivity statistics
        if self.predicate_selectivities:
            features.extend([
                np.mean(self.predicate_selectivities),
                np.std(self.predicate_selectivities),
                min(self.predicate_selectivities),
                max(self.predicate_selectivities)
            ])
        else:
            features.extend([1.0, 0.0, 1.0, 1.0])
            
        # Cardinality statistics
        if self.column_cardinalities:
            features.extend([
                np.mean(self.column_cardinalities),
                np.std(self.column_cardinalities),
                max(self.column_cardinalities)
            ])
        else:
            features.extend([1, 0, 1])
            
        # Index availability ratio
        index_ratio = np.mean(self.index_availability) if self.index_availability else 0.0
        features.append(index_ratio)
        
        return np.array(features, dtype=np.float32)


class CardinalityModel:
    """
    XGBoost-based cardinality estimation model.
    
    This model learns to predict the number of rows returned by
    query operators based on query features and historical execution data.
    """
    
    def __init__(self, model_path: str = None):
        self.model = None
        self.model_path = model_path or "cardinality_model.pkl"
        self.feature_names = []
        self.training_data = []
        self.is_trained = False
        
        # Load existing model if available
        if os.path.exists(self.model_path):
            self.load_model()
    
    def extract_features(self, query_plan: Dict, statistics_collector) -> QueryFeatures:
        """Extract features from a query plan for cardinality estimation."""
        # Initialize counters
        table_count = 0
        join_count = 0
        predicate_count = 0
        aggregate_count = 0
        table_sizes = []
        join_types = []
        predicate_selectivities = []
        column_cardinalities = []
        index_availability = []
        
        # Recursive feature extraction
        def extract_from_plan(plan: Dict):
            nonlocal table_count, join_count, predicate_count, aggregate_count
            
            plan_type = plan.get('type', '')
            
            if plan_type in ('SELECT', 'TABLE_SCAN', 'INDEX_SCAN'):
                table_count += 1
                table_name = plan.get('table', '')
                if table_name:
                    try:
                        stats = statistics_collector.collect_table_statistics(table_name)
                        table_sizes.append(stats.row_count)
                    except:
                        table_sizes.append(1000)  # Default
                        
                # Count predicates
                if 'condition' in plan:
                    predicate_count += 1
                    # Estimate selectivity (simplified)
                    selectivity = self._estimate_selectivity(plan['condition'])
                    predicate_selectivities.append(selectivity)
                    
            elif plan_type == 'JOIN':
                join_count += 1
                join_type = plan.get('join_type', 'INNER')
                join_types.append(join_type)
                
                # Process both sides
                if 'left_plan' in plan:
                    extract_from_plan(plan['left_plan'])
                if 'right_plan' in plan:
                    extract_from_plan(plan['right_plan'])
                    
            elif plan_type in ('AGGREGATE', 'GROUP_BY'):
                aggregate_count += 1
                
            # Process child plans
            if 'child' in plan and isinstance(plan['child'], dict):
                extract_from_plan(plan['child'])
        
        extract_from_plan(query_plan)
        
        # Calculate complexity score
        complexity_score = (
            table_count * 1.0 +
            join_count * 2.0 +
            predicate_count * 0.5 +
            aggregate_count * 1.5
        )
        
        return QueryFeatures(
            table_count=table_count,
            join_count=join_count,
            predicate_count=predicate_count,
            aggregate_count=aggregate_count,
            table_sizes=table_sizes,
            join_types=join_types,
            predicate_selectivities=predicate_selectivities,
            column_cardinalities=column_cardinalities,
            index_availability=index_availability,
            query_complexity_score=complexity_score
        )
    
    def _estimate_selectivity(self, condition: str) -> float:
        """Simple selectivity estimation for feature extraction."""
        if not condition:
            return 1.0
        
        # Count operations
        eq_count = condition.count('=')
        lt_count = condition.count('<')
        gt_count = condition.count('>')
        like_count = condition.upper().count('LIKE')
        
        # Simple heuristic
        if eq_count > 0:
            return 0.1 ** eq_count  # Equality is very selective
        elif lt_count + gt_count > 0:
            return 0.3  # Range conditions
        elif like_count > 0:
            return 0.2  # Pattern matching
        else:
            return 0.5  # Default
    
    def predict_cardinality(self, query_features: QueryFeatures) -> int:
        """Predict cardinality for a query based on features."""
        if not HAS_XGBOOST or not self.is_trained:
            # Fallback to simple heuristic
            return max(1, int(np.mean(query_features.table_sizes) * 0.1))
        
        try:
            feature_vector = query_features.to_vector().reshape(1, -1)
            prediction = self.model.predict(feature_vector)[0]
            return max(1, int(prediction))
        except Exception as e:
            logging.warning(f"ML cardinality prediction failed: {e}")
            return max(1, int(np.mean(query_features.table_sizes) * 0.1))
    
    def add_training_example(self, query_features: QueryFeatures, actual_cardinality: int):
        """Add a training example for model improvement."""
        self.training_data.append((query_features.to_vector(), actual_cardinality))
        
        # Retrain periodically
        if len(self.training_data) % 100 == 0:
            self.train_model()
    
    def train_model(self):
        """Train the XGBoost model on collected data."""
        if not HAS_XGBOOST or len(self.training_data) < 10:
            return
        
        try:
            # Prepare training data
            X = np.array([example[0] for example in self.training_data])
            y = np.array([example[1] for example in self.training_data])
            
            # Train XGBoost model
            self.model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            )
            
            self.model.fit(X, y)
            self.is_trained = True
            
            # Save model
            self.save_model()
            
            logging.info(f"Trained cardinality model on {len(self.training_data)} examples")
            
        except Exception as e:
            logging.error(f"Failed to train cardinality model: {e}")
    
    def save_model(self):
        """Save the trained model to disk."""
        if self.model:
            try:
                with open(self.model_path, 'wb') as f:
                    pickle.dump({
                        'model': self.model,
                        'is_trained': self.is_trained,
                        'training_data_count': len(self.training_data)
                    }, f)
            except Exception as e:
                logging.error(f"Failed to save cardinality model: {e}")
    
    def load_model(self):
        """Load a previously trained model from disk."""
        try:
            with open(self.model_path, 'rb') as f:
                data = pickle.load(f)
                self.model = data['model']
                self.is_trained = data['is_trained']
                logging.info(f"Loaded cardinality model (trained on {data.get('training_data_count', 0)} examples)")
        except Exception as e:
            logging.warning(f"Failed to load cardinality model: {e}")


class JoinOrderPolicyModel:
    """
    Neural network-based join order policy model.
    
    This model uses attention mechanisms to learn optimal join orderings
    based on query structure and table statistics.
    """
    
    def __init__(self, model_path: str = None):
        self.model = None
        self.model_path = model_path or "join_order_model.pt"
        self.optimizer = None
        self.is_trained = False
        self.device = torch.device('cpu')
        
        if HAS_PYTORCH:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self._init_model()
            
            # Load existing model if available
            if os.path.exists(self.model_path):
                self.load_model()
    
    def _init_model(self):
        """Initialize the neural network model."""
        if not HAS_PYTORCH:
            return
            
        class JoinOrderNet(nn.Module):
            def __init__(self, input_dim=64, hidden_dim=128):
                super().__init__()
                self.input_dim = input_dim
                self.hidden_dim = hidden_dim
                
                # Encoder layers
                self.encoder = nn.Sequential(
                    nn.Linear(input_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Dropout(0.1),
                    nn.Linear(hidden_dim, hidden_dim),
                    nn.ReLU(),
                    nn.Dropout(0.1)
                )
                
                # Attention mechanism
                self.attention = nn.MultiheadAttention(
                    embed_dim=hidden_dim,
                    num_heads=8,
                    batch_first=True
                )
                
                # Policy head (outputs action probabilities)
                self.policy_head = nn.Sequential(
                    nn.Linear(hidden_dim, hidden_dim // 2),
                    nn.ReLU(),
                    nn.Linear(hidden_dim // 2, 1)
                )
                
            def forward(self, x):
                # x shape: (batch_size, seq_len, input_dim)
                batch_size, seq_len, _ = x.shape
                
                # Encode each position
                x_flat = x.view(-1, self.input_dim)
                encoded = self.encoder(x_flat)
                encoded = encoded.view(batch_size, seq_len, self.hidden_dim)
                
                # Apply attention
                attended, _ = self.attention(encoded, encoded, encoded)
                
                # Generate policy scores
                scores = self.policy_head(attended)
                scores = scores.squeeze(-1)  # (batch_size, seq_len)
                
                return torch.softmax(scores, dim=-1)
        
        self.model = JoinOrderNet().to(self.device)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
    
    def predict_join_order(self, tables: List[str], join_graph: Dict, 
                          statistics_collector) -> List[str]:
        """Predict optimal join order using the trained model."""
        if not HAS_PYTORCH or not self.is_trained or len(tables) <= 2:
            # Fallback to simple heuristic: join smallest tables first
            table_sizes = []
            for table in tables:
                try:
                    stats = statistics_collector.collect_table_statistics(table)
                    table_sizes.append((table, stats.row_count))
                except:
                    table_sizes.append((table, 1000))
            
            # Sort by size (smallest first)
            table_sizes.sort(key=lambda x: x[1])
            return [table for table, _ in table_sizes]
        
        try:
            # Extract features for each table
            features = []
            for table in tables:
                table_features = self._extract_table_features(
                    table, tables, join_graph, statistics_collector
                )
                features.append(table_features)
            
            # Convert to tensor
            feature_tensor = torch.tensor(features, dtype=torch.float32).unsqueeze(0)
            feature_tensor = feature_tensor.to(self.device)
            
            # Get model predictions
            with torch.no_grad():
                probabilities = self.model(feature_tensor)
                probabilities = probabilities.squeeze(0).cpu().numpy()
            
            # Sort tables by probability (highest first for join order)
            table_probs = list(zip(tables, probabilities))
            table_probs.sort(key=lambda x: x[1], reverse=True)
            
            return [table for table, _ in table_probs]
            
        except Exception as e:
            logging.warning(f"ML join order prediction failed: {e}")
            # Fallback to size-based ordering
            return self.predict_join_order(tables, join_graph, statistics_collector)
    
    def _extract_table_features(self, table: str, all_tables: List[str], 
                               join_graph: Dict, statistics_collector) -> np.ndarray:
        """Extract features for a single table in the join context."""
        features = []
        
        try:
            # Table statistics
            stats = statistics_collector.collect_table_statistics(table)
            features.extend([
                math.log10(max(1, stats.row_count)),  # Log-scaled row count
                stats.avg_row_length / 100.0,  # Normalized row width
                len(stats.columns),  # Number of columns
            ])
        except:
            features.extend([3.0, 1.0, 5.0])  # Default values
        
        # Join connectivity
        connected_tables = len(join_graph.get(table, set()))
        total_possible = len(all_tables) - 1
        connectivity_ratio = connected_tables / max(1, total_possible)
        features.append(connectivity_ratio)
        
        # Position features (normalized)
        table_index = all_tables.index(table) if table in all_tables else 0
        position_ratio = table_index / max(1, len(all_tables) - 1)
        features.append(position_ratio)
        
        # Pad to fixed size (64 features)
        while len(features) < 64:
            features.append(0.0)
        
        return np.array(features[:64], dtype=np.float32)
    
    def add_training_example(self, tables: List[str], join_graph: Dict,
                           optimal_order: List[str], cost: float, 
                           statistics_collector):
        """Add a training example for reinforcement learning."""
        if not HAS_PYTORCH:
            return
        
        # This would implement experience replay for RL training
        # For now, we'll implement a simplified version
        pass
    
    def save_model(self):
        """Save the trained model to disk."""
        if HAS_PYTORCH and self.model:
            try:
                torch.save({
                    'model_state_dict': self.model.state_dict(),
                    'optimizer_state_dict': self.optimizer.state_dict(),
                    'is_trained': self.is_trained
                }, self.model_path)
            except Exception as e:
                logging.error(f"Failed to save join order model: {e}")
    
    def load_model(self):
        """Load a previously trained model from disk."""
        if not HAS_PYTORCH:
            return
            
        try:
            checkpoint = torch.load(self.model_path, map_location=self.device)
            self.model.load_state_dict(checkpoint['model_state_dict'])
            self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
            self.is_trained = checkpoint.get('is_trained', False)
            logging.info("Loaded join order policy model")
        except Exception as e:
            logging.warning(f"Failed to load join order model: {e}")


class MLExtensionManager:
    """
    Manager for all ML-based query optimization extensions.
    
    This class coordinates the various ML models and provides
    a unified interface for the main optimizer.
    """
    
    def __init__(self, enable_cardinality_model: bool = True,
                 enable_join_order_model: bool = True):
        self.enable_cardinality_model = enable_cardinality_model and HAS_XGBOOST
        self.enable_join_order_model = enable_join_order_model and HAS_PYTORCH
        
        # Initialize models
        self.cardinality_model = None
        self.join_order_model = None
        
        if self.enable_cardinality_model:
            self.cardinality_model = CardinalityModel()
            
        if self.enable_join_order_model:
            self.join_order_model = JoinOrderPolicyModel()
        
        logging.info(f"ML Extensions initialized - Cardinality: {self.enable_cardinality_model}, "
                    f"Join Order: {self.enable_join_order_model}")
    
    def estimate_cardinality(self, query_plan: Dict, statistics_collector) -> Optional[int]:
        """Estimate cardinality using ML model if available."""
        if not self.cardinality_model:
            return None
        
        try:
            features = self.cardinality_model.extract_features(query_plan, statistics_collector)
            return self.cardinality_model.predict_cardinality(features)
        except Exception as e:
            logging.warning(f"ML cardinality estimation failed: {e}")
            return None
    
    def suggest_join_order(self, tables: List[str], join_graph: Dict,
                          statistics_collector) -> Optional[List[str]]:
        """Suggest join order using ML model if available."""
        if not self.join_order_model or len(tables) <= 2:
            return None
        
        try:
            return self.join_order_model.predict_join_order(
                tables, join_graph, statistics_collector
            )
        except Exception as e:
            logging.warning(f"ML join order suggestion failed: {e}")
            return None
    
    def add_execution_feedback(self, query_plan: Dict, actual_cardinality: int,
                              actual_cost: float, statistics_collector):
        """Add execution feedback to improve models."""
        if self.cardinality_model:
            try:
                features = self.cardinality_model.extract_features(query_plan, statistics_collector)
                self.cardinality_model.add_training_example(features, actual_cardinality)
            except Exception as e:
                logging.warning(f"Failed to add cardinality feedback: {e}")
    
    def get_model_status(self) -> Dict[str, Any]:
        """Get status information about the ML models."""
        status = {
            'cardinality_model_enabled': self.enable_cardinality_model,
            'join_order_model_enabled': self.enable_join_order_model,
            'xgboost_available': HAS_XGBOOST,
            'pytorch_available': HAS_PYTORCH
        }
        
        if self.cardinality_model:
            status['cardinality_model_trained'] = self.cardinality_model.is_trained
            status['cardinality_training_examples'] = len(self.cardinality_model.training_data)
        
        if self.join_order_model:
            status['join_order_model_trained'] = self.join_order_model.is_trained
        
        return status
