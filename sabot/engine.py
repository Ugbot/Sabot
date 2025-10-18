#!/usr/bin/env python3
"""
Unified Sabot Engine

Single entry point for all Sabot functionality (Stream, SQL, Graph processing).
Provides consistent API and manages underlying execution infrastructure.
"""

import logging
from typing import Optional, Dict, Any, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class Sabot:
    """
    Unified Sabot engine for stream, SQL, and graph processing.
    
    Provides a single entry point for all Sabot functionality with consistent
    API across different processing paradigms.
    
    Example:
        # Local mode (single machine)
        engine = Sabot(mode='local')
        
        # Distributed mode (cluster)
        engine = Sabot(mode='distributed', coordinator='localhost:8080')
        
        # Stream processing
        stream = engine.stream.from_kafka('topic').filter(lambda b: b.column('x') > 10)
        
        # SQL processing
        result = engine.sql("SELECT * FROM table WHERE x > 10")
        
        # Graph processing
        matches = engine.graph.cypher("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
    """
    
    def __init__(
        self,
        mode: str = 'local',
        state_path: Optional[Union[str, Path]] = None,
        coordinator: Optional[str] = None,
        **config
    ):
        """
        Initialize Sabot engine.
        
        Args:
            mode: 'local' for single-machine or 'distributed' for cluster
            state_path: Path to state storage (default: './sabot_state')
            coordinator: Coordinator address for distributed mode (host:port)
            **config: Additional configuration options
        """
        self.mode = mode
        self.config = config
        self.config['state_path'] = state_path or './sabot_state'
        self.config['coordinator'] = coordinator
        
        # Core components
        self._state = None
        self._orchestrator = None
        self._operator_registry = None
        self._shuffle_service = None
        
        # Initialize layers
        self._initialize_engine()
        
        # Create API facades
        self._stream_api = None
        self._sql_api = None
        self._graph_api = None
        
        logger.info(f"Initialized Sabot engine in {mode} mode")
    
    def _initialize_engine(self):
        """Initialize core engine components."""
        # Initialize in dependency order
        self._init_state()
        self._init_operator_registry()
        self._init_shuffle_service()
        self._init_orchestrator()
    
    def _init_state(self):
        """Initialize state management layer."""
        try:
            from sabot.state.manager import StateManager
            
            self._state = StateManager(self.config)
            logger.info(f"Initialized state backend: {self._state.backend.__class__.__name__}")
            
        except ImportError as e:
            logger.warning(f"State manager import failed: {e}, using memory backend")
            # Fallback to memory backend
            try:
                from sabot.stores.memory import MemoryBackend
                self._state = type('StateManager', (), {'backend': MemoryBackend()})()
            except:
                # Ultimate fallback - simple dict wrapper
                class SimpleMemory:
                    def __init__(self):
                        self._data = {}
                    async def get(self, key):
                        return self._data.get(key)
                    async def put(self, key, value):
                        self._data[key] = value
                    def get_backend_type(self):
                        return 'memory'
                self._state = type('StateManager', (), {'backend': SimpleMemory()})()
        except Exception as e:
            logger.error(f"Failed to initialize state backend: {e}")
            raise    
    def _init_operator_registry(self):
        """Initialize operator registry."""
        try:
            from sabot.operators.registry import create_default_registry
            
            self._operator_registry = create_default_registry()
            operator_count = len(self._operator_registry.list_operators())
            logger.info(f"Initialized operator registry with {operator_count} operators")
            
        except Exception as e:
            logger.warning(f"Failed to initialize operator registry: {e}")
            self._operator_registry = None
    
    def _init_shuffle_service(self):
        """Initialize shuffle service for distributed operations."""
        if self.mode == 'local':
            logger.debug("Local mode: shuffle service not needed")
            return
        
        try:
            from sabot.orchestrator.shuffle import ShuffleService
            
            shuffle_config = {
                'compression': self.config.get('shuffle_compression', 'lz4'),
                'spill_threshold': self.config.get('shuffle_spill_threshold', 100 * 1024 * 1024),  # 100MB
            }
            
            self._shuffle_service = ShuffleService(shuffle_config)
            logger.info("Initialized unified shuffle service")
            
        except ImportError as e:
            logger.warning(f"Shuffle service not available: {e}")
            self._shuffle_service = None
        except Exception as e:
            logger.warning(f"Failed to initialize shuffle service: {e}")
            self._shuffle_service = None
    
    def _init_orchestrator(self):
        """Initialize orchestration layer."""
        if self.mode == 'local':
            logger.debug("Local mode: orchestrator not needed")
            return
        
        try:
            # Try unified coordinator first (new architecture)
            from sabot.orchestrator.coordinator_unified import UnifiedCoordinator
            
            self._orchestrator = UnifiedCoordinator(
                mode='embedded',
                state_backend=self._state.backend if self._state else None,
                shuffle_service=self._shuffle_service,
                config=self.config
            )
            logger.info("Initialized unified orchestrator")
            
        except ImportError:
            # Fallback to JobManager (old architecture, for backward compatibility)
            try:
                from sabot.job_manager import JobManager
                
                # JobManager has different init signature, adapt it
                self._orchestrator = JobManager(
                    # Pass what JobManager expects
                    config=self.config
                )
                logger.info("Initialized job orchestrator (legacy JobManager)")
                
            except Exception as e:
                logger.warning(f"Failed to initialize orchestrator: {e}")
                self._orchestrator = None
        except Exception as e:
            logger.warning(f"Failed to initialize orchestrator: {e}")
            self._orchestrator = None
    
    @property
    def stream(self):
        """
        Access Stream API.
        
        Returns:
            StreamAPI: Stream processing interface
            
        Example:
            stream = engine.stream.from_kafka('topic').filter(lambda b: b.column('x') > 0)
        """
        if self._stream_api is None:
            from sabot.api.stream_facade import StreamAPI
            self._stream_api = StreamAPI(self)
        return self._stream_api
    
    @property
    def sql(self):
        """
        Access SQL API.
        
        Returns:
            SQLAPI: SQL processing interface
            
        Example:
            result = engine.sql("SELECT * FROM table WHERE x > 10")
        """
        if self._sql_api is None:
            from sabot.api.sql_facade import SQLAPI
            self._sql_api = SQLAPI(self)
        return self._sql_api
    
    @property
    def graph(self):
        """
        Access Graph API.
        
        Returns:
            GraphAPI: Graph processing interface
            
        Example:
            matches = engine.graph.cypher("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
        """
        if self._graph_api is None:
            from sabot.api.graph_facade import GraphAPI
            self._graph_api = GraphAPI(self)
        return self._graph_api
    
    def execute_query(self, query: str, language: str = 'auto'):
        """
        Execute a query (auto-detects SQL vs Graph).
        
        Args:
            query: Query string
            language: 'sql', 'cypher', 'sparql', or 'auto' (auto-detect)
            
        Returns:
            Query results
            
        Example:
            engine.execute_query("SELECT * FROM table")
            engine.execute_query("MATCH (a)-[:R]->(b) RETURN a", language='cypher')
        """
        if language == 'auto':
            language = self._detect_query_language(query)
        
        if language == 'sql':
            return self.sql.execute(query)
        elif language == 'cypher':
            return self.graph.cypher(query)
        elif language == 'sparql':
            return self.graph.sparql(query)
        else:
            raise ValueError(f"Unknown query language: {language}")
    
    def _detect_query_language(self, query: str) -> str:
        """Auto-detect query language."""
        query_upper = query.strip().upper()
        
        if query_upper.startswith('SELECT') or query_upper.startswith('WITH'):
            return 'sql'
        elif query_upper.startswith('MATCH') or query_upper.startswith('CREATE'):
            return 'cypher'
        elif query_upper.startswith('PREFIX') or 'WHERE' in query_upper and '{' in query:
            return 'sparql'
        else:
            # Default to SQL
            return 'sql'
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get engine statistics.
        
        Returns:
            Dictionary with engine stats
        """
        stats = {
            'mode': self.mode,
            'state_backend': self._state.backend.__class__.__name__ if self._state else None,
            'operators_registered': len(self._operator_registry.list_operators()) if self._operator_registry else 0,
        }
        
        if self._orchestrator:
            stats['orchestrator'] = 'JobManager'
            # Could add orchestrator stats here
        
        if self._shuffle_service:
            stats['shuffle_enabled'] = True
        
        return stats
    
    def shutdown(self):
        """Gracefully shutdown engine and cleanup resources."""
        logger.info("Shutting down Sabot engine")
        
        # Shutdown in reverse dependency order
        if self._orchestrator:
            try:
                # Orchestrator shutdown if it has one
                if hasattr(self._orchestrator, 'shutdown'):
                    self._orchestrator.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down orchestrator: {e}")
        
        if self._shuffle_service:
            try:
                if hasattr(self._shuffle_service, 'stop'):
                    self._shuffle_service.stop()
            except Exception as e:
                logger.error(f"Error shutting down shuffle service: {e}")
        
        if self._state:
            try:
                if hasattr(self._state.backend, 'close'):
                    self._state.backend.close()
            except Exception as e:
                logger.error(f"Error closing state backend: {e}")
        
        logger.info("Sabot engine shutdown complete")
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.shutdown()
        return False
    
    def __repr__(self):
        return f"Sabot(mode='{self.mode}', state={self._state.backend.__class__.__name__ if self._state else 'None'})"


# Convenience function for quick start
def create_engine(mode='local', **config) -> Sabot:
    """
    Create a Sabot engine.
    
    Args:
        mode: 'local' or 'distributed'
        **config: Engine configuration
        
    Returns:
        Sabot: Initialized engine
        
    Example:
        engine = create_engine('local')
        engine = create_engine('distributed', coordinator='localhost:8080')
    """
    return Sabot(mode=mode, **config)

