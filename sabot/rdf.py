"""
Sabot RDF Store and SPARQL Engine

User-friendly Python API for RDF triple storage and SPARQL query execution.

Example:
    >>> from sabot.rdf import RDFStore
    >>>
    >>> # Create store
    >>> store = RDFStore()
    >>>
    >>> # Add triples
    >>> store.add("http://example.org/Alice", "http://xmlns.com/foaf/0.1/name", "Alice")
    >>>
    >>> # Query with SPARQL
    >>> results = store.query('''
    ...     PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    ...     SELECT ?person ?name
    ...     WHERE { ?person foaf:name ?name . }
    ... ''')
    >>>
    >>> print(results.to_pandas())
"""

from typing import Optional, Union, List, Tuple, Dict, Any
import tempfile
import os
from sabot import cyarrow as pa
from sabot._cython.graph.compiler.sparql_parser import SPARQLParser
from sabot._cython.graph.storage.graph_storage import PyRDFTripleStore
from sabot._cython.graph.compiler.sparql_translator import SPARQLTranslator

# Try to import C++ bindings
try:
    import sys
    import importlib.util
    import glob

    # Get absolute path to the bindings directory
    bindings_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sabot_ql', 'bindings', 'python'))

    # Find the .so file (support different Python versions)
    so_files = glob.glob(os.path.join(bindings_path, 'sabot_ql', 'bindings', 'python', 'sabot_ql.cpython-*.so'))
    if not so_files:
        raise FileNotFoundError("C++ bindings .so file not found")

    module_path = so_files[0]

    # Load the module directly from the .so file with correct module name
    spec = importlib.util.spec_from_file_location("sabot_ql.bindings.python.sabot_ql", module_path)
    sabot_ql_native = importlib.util.module_from_spec(spec)
    sys.modules['sabot_ql.bindings.python.sabot_ql'] = sabot_ql_native
    spec.loader.exec_module(sabot_ql_native)
    CPP_BACKEND_AVAILABLE = True
except (ImportError, FileNotFoundError, AttributeError) as e:
    CPP_BACKEND_AVAILABLE = False
    sabot_ql_native = None


class RDFStore:
    """
    High-level RDF triple store with SPARQL query support.

    Features:
    - Zero-copy Arrow storage
    - 3-index strategy (SPO, POS, OSP) for fast pattern matching
    - Full SPARQL 1.1 query support
    - Automatic vocabulary management
    - 3-37M matches/sec throughput

    Attributes:
        store: Underlying PyRDFTripleStore instance
        parser: SPARQL parser instance
        translator: SPARQL query translator
        prefixes: Registered PREFIX declarations
    """

    def __init__(self, backend='cpp'):
        """
        Initialize empty RDF store.

        Args:
            backend: Storage/query backend - 'cpp' (fast, recommended) or 'python' (slow, deprecated)
                     - 'cpp': Uses C++ W3C SPARQL 1.1 engine (30-50x faster)
                     - 'python': Uses Python implementation (deprecated, O(n²) scaling)
        """
        self.backend = backend

        # Validate backend choice
        if backend == 'cpp' and not CPP_BACKEND_AVAILABLE:
            raise ValueError(
                "C++ backend not available. C++ bindings not built.\n"
                "Falling back to 'python' backend or rebuild C++ bindings."
            )

        if backend == 'cpp':
            # C++ backend - create temp MarbleDB
            self._temp_dir = tempfile.mkdtemp(prefix='sabot_rdf_')
            self._cpp_store = sabot_ql_native.create_triple_store(self._temp_dir)
            self._data_loaded = False

            # Track terms/triples for lazy loading
            self._term_counter = 0
            self._term_to_id = {}
            self._id_to_term = {}
            self._terms_list = []
            self._triples_list = []

            # Python components not needed
            self.store = None
            self.parser = None
            self.translator = None
            self._triples_table = None
            self._terms_table = None
        else:
            # Python backend (original implementation)
            self._temp_dir = None
            self._cpp_store = None
            self._data_loaded = False

            # Start with empty store - will be created on first add
            self._triples_table = None
            self._terms_table = None
            self.store = None
            self.parser = SPARQLParser()
            self.translator = None

            # Track terms for vocabulary
            self._term_counter = 0
            self._term_to_id = {}  # lex -> id
            self._id_to_term = {}  # id -> lex
            self._terms_list = []  # List of (id, lex, kind, lang, datatype)

            # Track triples
            self._triples_list = []  # List of (s, p, o) as IDs

        # Common prefixes (both backends)
        self.prefixes = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'owl': 'http://www.w3.org/2002/07/owl#',
        }

    def add_prefix(self, prefix: str, namespace: str):
        """
        Register a PREFIX for use in SPARQL queries.

        Args:
            prefix: Short prefix name (e.g., 'foaf')
            namespace: Full namespace URI (e.g., 'http://xmlns.com/foaf/0.1/')
        """
        self.prefixes[prefix] = namespace

    def _get_or_create_term_id(self, lex: str, is_literal: bool = False,
                                lang: str = '', datatype: str = '') -> int:
        """Get existing term ID or create new one."""
        key = (lex, is_literal, lang, datatype)
        if key not in self._term_to_id:
            # Create new term ID
            # IRIs get high bit set (>= 2^62), literals don't
            if is_literal:
                term_id = self._term_counter
            else:
                term_id = self._term_counter | (1 << 62)

            self._term_counter += 1
            self._term_to_id[key] = term_id
            self._id_to_term[term_id] = lex

            kind = 1 if is_literal else 0  # 0=IRI, 1=Literal
            self._terms_list.append((term_id, lex, kind, lang, datatype))

            return term_id
        return self._term_to_id[key]

    def add(self, subject: str, predicate: str, obj: str,
            obj_is_literal: bool = False, lang: str = '', datatype: str = ''):
        """
        Add a single RDF triple to the store.

        Args:
            subject: Subject URI (IRI)
            predicate: Predicate URI (IRI)
            obj: Object URI or literal value
            obj_is_literal: True if object is a literal, False if IRI
            lang: Language tag for literal (e.g., 'en')
            datatype: Datatype URI for literal (e.g., 'xsd:integer')

        Example:
            >>> store.add("http://example.org/Alice",
            ...          "http://xmlns.com/foaf/0.1/name",
            ...          "Alice", obj_is_literal=True)
        """
        # Get or create term IDs
        s_id = self._get_or_create_term_id(subject, is_literal=False)
        p_id = self._get_or_create_term_id(predicate, is_literal=False)
        o_id = self._get_or_create_term_id(obj, is_literal=obj_is_literal,
                                           lang=lang, datatype=datatype)

        # Add triple
        self._triples_list.append((s_id, p_id, o_id))

        # Rebuild store
        self._rebuild_store()

    def add_many(self, triples: List[Tuple[str, str, str, bool]]):
        """
        Add multiple triples efficiently.

        Args:
            triples: List of (subject, predicate, object, obj_is_literal) tuples

        Example:
            >>> triples = [
            ...     ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/name", "Alice", True),
            ...     ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/age", "25", True),
            ... ]
            >>> store.add_many(triples)
        """
        for triple in triples:
            if len(triple) == 4:
                s, p, o, is_lit = triple
                lang, dtype = '', ''
            elif len(triple) == 6:
                s, p, o, is_lit, lang, dtype = triple
            else:
                raise ValueError(f"Invalid triple format: {triple}")

            s_id = self._get_or_create_term_id(s, is_literal=False)
            p_id = self._get_or_create_term_id(p, is_literal=False)
            o_id = self._get_or_create_term_id(o, is_literal=is_lit,
                                               lang=lang, datatype=dtype)
            self._triples_list.append((s_id, p_id, o_id))

        self._rebuild_store()

    def _rebuild_store(self):
        """Rebuild Arrow tables and PyRDFTripleStore from accumulated data."""
        if not self._triples_list:
            return

        # Build terms table
        terms_data = {
            'id': [t[0] for t in self._terms_list],
            'lex': [t[1] for t in self._terms_list],
            'kind': [t[2] for t in self._terms_list],
            'lang': [t[3] for t in self._terms_list],
            'datatype': [t[4] for t in self._terms_list],
        }
        self._terms_table = pa.Table.from_pydict({
            'id': pa.array(terms_data['id'], type=pa.int64()),
            'lex': pa.array(terms_data['lex'], type=pa.string()),
            'kind': pa.array(terms_data['kind'], type=pa.uint8()),
            'lang': pa.array(terms_data['lang'], type=pa.string()),
            'datatype': pa.array(terms_data['datatype'], type=pa.string()),
        })

        # Build triples table
        triples_data = {
            's': [t[0] for t in self._triples_list],
            'p': [t[1] for t in self._triples_list],
            'o': [t[2] for t in self._triples_list],
        }
        self._triples_table = pa.Table.from_pydict({
            's': pa.array(triples_data['s'], type=pa.int64()),
            'p': pa.array(triples_data['p'], type=pa.int64()),
            'o': pa.array(triples_data['o'], type=pa.int64()),
        })

        # Create store
        self.store = PyRDFTripleStore(self._triples_table, self._terms_table)
        self.translator = SPARQLTranslator(self.store)

    def _load_to_cpp(self):
        """Load accumulated triples/terms into C++ store (for cpp backend only)."""
        if not self._triples_list:
            return

        # Build Arrow tables from accumulated data
        # Build terms table
        terms_data = {
            'id': [t[0] for t in self._terms_list],
            'lex': [t[1] for t in self._terms_list],
            'kind': [t[2] for t in self._terms_list],
            'lang': [t[3] for t in self._terms_list],
            'datatype': [t[4] for t in self._terms_list],
        }
        terms_table = pa.Table.from_pydict({
            'id': pa.array(terms_data['id'], type=pa.int64()),
            'lex': pa.array(terms_data['lex'], type=pa.string()),
            'kind': pa.array(terms_data['kind'], type=pa.uint8()),
            'lang': pa.array(terms_data['lang'], type=pa.string()),
            'datatype': pa.array(terms_data['datatype'], type=pa.string()),
        })

        # Build triples table
        triples_data = {
            'subject': [t[0] for t in self._triples_list],
            'predicate': [t[1] for t in self._triples_list],
            'object': [t[2] for t in self._triples_list],
        }
        triples_table = pa.Table.from_pydict({
            'subject': pa.array(triples_data['subject'], type=pa.int64()),
            'predicate': pa.array(triples_data['predicate'], type=pa.int64()),
            'object': pa.array(triples_data['object'], type=pa.int64()),
        })

        # Load into C++ store
        self._cpp_store.load_data(triples_table, terms_table)
        self._data_loaded = True

    def query(self, sparql: str) -> pa.Table:
        """
        Execute a SPARQL query and return results as Arrow table.

        Args:
            sparql: SPARQL query string (SELECT, PREFIX, WHERE, etc.)

        Returns:
            Arrow table with query results

        Raises:
            ValueError: If query is invalid or store is empty

        Example:
            >>> results = store.query('''
            ...     PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            ...     SELECT ?person ?name
            ...     WHERE { ?person foaf:name ?name . }
            ... ''')
        """
        if self.backend == 'cpp':
            # C++ backend
            if not self._triples_list:
                raise ValueError("Store is empty - add triples before querying")

            # Load data into C++ store if not already done
            if not self._data_loaded:
                self._load_to_cpp()

            # Execute query using C++ engine
            try:
                result = self._cpp_store.query(sparql)
                return result
            except Exception as e:
                raise ValueError(f"Failed to execute query (C++ backend): {e}")
        else:
            # Python backend (original implementation)
            if self.store is None:
                raise ValueError("Store is empty - add triples before querying")

            # Parse query
            try:
                ast = self.parser.parse(sparql)
            except Exception as e:
                raise ValueError(f"Failed to parse SPARQL query: {e}")

            # Execute query
            try:
                result = self.translator.execute(ast)
                return result
            except Exception as e:
                raise ValueError(f"Failed to execute query: {e}")

    def filter_triples(self, subject: Optional[str] = None,
                      predicate: Optional[str] = None,
                      obj: Optional[str] = None) -> pa.Table:
        """
        Direct triple pattern matching (bypass SPARQL parser).

        Args:
            subject: Subject URI (None = wildcard)
            predicate: Predicate URI (None = wildcard)
            obj: Object URI or literal (None = wildcard)

        Returns:
            Arrow table with columns [s, p, o] containing matching triples

        Example:
            >>> # Find all triples with foaf:name predicate
            >>> results = store.filter_triples(
            ...     predicate="http://xmlns.com/foaf/0.1/name"
            ... )
        """
        if self.store is None:
            raise ValueError("Store is empty")

        # Convert URIs to IDs
        s_id = self._term_to_id.get((subject, False, '', ''), -1) if subject else -1
        p_id = self._term_to_id.get((predicate, False, '', ''), -1) if predicate else -1
        o_id = -1
        if obj:
            # Try as IRI first, then as literal
            o_id = self._term_to_id.get((obj, False, '', ''), -1)
            if o_id == -1:
                o_id = self._term_to_id.get((obj, True, '', ''), -1)

        return self.store.filter_triples(s_id, p_id, o_id)

    def count(self) -> int:
        """Get total number of triples in store."""
        return len(self._triples_list)

    def count_terms(self) -> int:
        """Get total number of unique terms (vocabulary size)."""
        return len(self._terms_list)

    def get_term(self, term_id: int) -> Optional[str]:
        """Get lexical value for a term ID."""
        return self._id_to_term.get(term_id)

    def stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        return {
            'num_triples': self.count(),
            'num_terms': self.count_terms(),
            'num_prefixes': len(self.prefixes),
            'has_store': self.store is not None,
        }

    def __repr__(self):
        backend_info = f", backend={self.backend}" if hasattr(self, 'backend') else ""
        return f"RDFStore(triples={self.count()}, terms={self.count_terms()}{backend_info})"

    def __str__(self):
        stats = self.stats()
        backend_info = f"\n  Backend: {self.backend}" if hasattr(self, 'backend') else ""
        return (f"RDFStore:\n"
                f"  Triples: {stats['num_triples']}\n"
                f"  Terms: {stats['num_terms']}\n"
                f"  Prefixes: {stats['num_prefixes']}{backend_info}")

    def __del__(self):
        """Cleanup temp directory if using C++ backend."""
        if hasattr(self, '_temp_dir') and self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass  # Ignore cleanup errors


class SPARQLEngine:
    """
    Standalone SPARQL query engine (for external RDF data sources).

    Use this when you have RDF data in Arrow format and want to query it
    with SPARQL without creating a full RDFStore.
    """

    def __init__(self, triples_table: pa.Table, terms_table: pa.Table):
        """
        Initialize SPARQL engine with Arrow tables.

        Args:
            triples_table: Arrow table with [s, p, o] int64 columns
            terms_table: Arrow table with [id, lex, kind, lang, datatype] columns
        """
        self.store = PyRDFTripleStore(triples_table, terms_table)
        self.parser = SPARQLParser()
        self.translator = SPARQLTranslator(self.store)

    def query(self, sparql: str) -> pa.Table:
        """
        Execute SPARQL query.

        Args:
            sparql: SPARQL query string

        Returns:
            Arrow table with results
        """
        ast = self.parser.parse(sparql)
        return self.translator.execute(ast)

    def filter_triples(self, subject: int = -1, predicate: int = -1,
                      obj: int = -1) -> pa.Table:
        """
        Direct pattern matching with term IDs.

        Args:
            subject: Subject term ID (-1 = wildcard)
            predicate: Predicate term ID (-1 = wildcard)
            obj: Object term ID (-1 = wildcard)

        Returns:
            Arrow table with matching triples
        """
        return self.store.filter_triples(subject, predicate, obj)


# Convenience function
def create_rdf_store() -> RDFStore:
    """Create a new RDF store."""
    return RDFStore()
