"""
RDF Data Loader for Sabot SPARQL Benchmarks

Converts RDF files (N-Triples, Turtle, RDF/XML) to Sabot's PyRDFTripleStore format.
Uses rdflib for parsing and PyArrow for efficient columnar storage.

Architecture:
1. Parse RDF file with rdflib
2. Build term dictionary (RDF term → int64 ID)
3. Convert triples to (subject_id, predicate_id, object_id) format
4. Create PyArrow tables for triples and term dictionary
5. Initialize PyRDFTripleStore with the tables

Dependencies:
- rdflib (pip install rdflib)
- pyarrow (vendored in Sabot)
"""

import sys
from pathlib import Path
from typing import Dict, Tuple, List
from collections import defaultdict

# Add Sabot to path
sabot_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(sabot_root))

# Use Sabot's vendored Arrow (cyarrow)
from sabot import cyarrow as pa

try:
    import rdflib
    from rdflib import Graph, URIRef, Literal, BNode
    from rdflib.namespace import RDF, RDFS, OWL, XSD
except ImportError:
    print("ERROR: rdflib not installed. Run: pip install rdflib")
    sys.exit(1)

from sabot._cython.graph.storage.graph_storage import PyRDFTripleStore


class TermDictionary:
    """
    Term dictionary for RDF terms → int64 IDs.

    Assigns unique IDs to IRIs, Literals, and Blank Nodes.
    Tracks term metadata (kind, language, datatype).
    """

    def __init__(self):
        self.term_to_id: Dict[str, int] = {}
        self.id_to_term: Dict[int, Tuple[str, int, str, str]] = {}
        self.next_id = 0

        # Term kinds
        self.KIND_IRI = 0
        self.KIND_LITERAL = 1
        self.KIND_BLANK = 2

    def add_term(self, term) -> int:
        """Add RDF term to dictionary and return its ID."""
        term_key = self._term_key(term)

        if term_key in self.term_to_id:
            return self.term_to_id[term_key]

        term_id = self.next_id
        self.next_id += 1

        # Extract term metadata
        if isinstance(term, URIRef):
            lex = str(term)
            kind = self.KIND_IRI
            lang = ""
            datatype = ""
        elif isinstance(term, Literal):
            lex = str(term)
            kind = self.KIND_LITERAL
            lang = term.language or ""
            datatype = str(term.datatype) if term.datatype else ""
        elif isinstance(term, BNode):
            lex = str(term)
            kind = self.KIND_BLANK
            lang = ""
            datatype = ""
        else:
            raise ValueError(f"Unknown term type: {type(term)}")

        self.term_to_id[term_key] = term_id
        self.id_to_term[term_id] = (lex, kind, lang, datatype)

        return term_id

    def _term_key(self, term) -> str:
        """Generate unique key for term (includes datatype/lang for literals)."""
        if isinstance(term, Literal):
            key = f"L:{term}"
            if term.language:
                key += f"@{term.language}"
            if term.datatype:
                key += f"^^{term.datatype}"
            return key
        else:
            return str(term)

    def to_arrow_table(self) -> pa.Table:
        """Convert term dictionary to Arrow table."""
        ids = []
        lexical_forms = []
        kinds = []
        languages = []
        datatypes = []

        for term_id in sorted(self.id_to_term.keys()):
            lex, kind, lang, datatype = self.id_to_term[term_id]
            ids.append(term_id)
            lexical_forms.append(lex)
            kinds.append(kind)
            languages.append(lang)
            datatypes.append(datatype)

        return pa.Table.from_pydict({
            'id': pa.array(ids, type=pa.int64()),
            'lex': pa.array(lexical_forms, type=pa.string()),
            'kind': pa.array(kinds, type=pa.uint8()),
            'lang': pa.array(languages, type=pa.string()),
            'datatype': pa.array(datatypes, type=pa.string())
        })


def load_rdf_file(filepath: str, format: str = 'nt') -> PyRDFTripleStore:
    """
    Load RDF file into Sabot's PyRDFTripleStore.

    Args:
        filepath: Path to RDF file
        format: RDF format ('nt' = N-Triples, 'turtle', 'xml', 'n3', 'trig')

    Returns:
        PyRDFTripleStore with loaded triples

    Example:
        >>> store = load_rdf_file('test_data.nt')
        >>> print(f"Loaded {store.num_triples()} triples")
    """
    print(f"Loading RDF file: {filepath} (format: {format})")

    # Parse RDF file with rdflib
    g = Graph()
    g.parse(filepath, format=format)

    print(f"  Parsed {len(g)} triples")

    # Build term dictionary
    term_dict = TermDictionary()

    # Convert triples to (s_id, p_id, o_id) format
    subjects = []
    predicates = []
    objects = []

    for s, p, o in g:
        s_id = term_dict.add_term(s)
        p_id = term_dict.add_term(p)
        o_id = term_dict.add_term(o)

        subjects.append(s_id)
        predicates.append(p_id)
        objects.append(o_id)

    print(f"  Built term dictionary: {term_dict.next_id} unique terms")

    # Create Arrow tables
    triples_table = pa.Table.from_pydict({
        's': pa.array(subjects, type=pa.int64()),
        'p': pa.array(predicates, type=pa.int64()),
        'o': pa.array(objects, type=pa.int64())
    })

    terms_table = term_dict.to_arrow_table()

    print(f"  Created Arrow tables:")
    print(f"    Triples: {triples_table.num_rows} rows")
    print(f"    Terms: {terms_table.num_rows} rows")

    # Create PyRDFTripleStore
    store = PyRDFTripleStore()
    store.load_from_tables(triples_table, terms_table)

    print(f"✅ Loaded RDF file into PyRDFTripleStore")

    return store


def load_krown_sample(sample_name: str) -> PyRDFTripleStore:
    """
    Load KROWN sample dataset.

    Args:
        sample_name: Name of KROWN sample directory (e.g., 'raw', 'duplicates')

    Returns:
        PyRDFTripleStore with loaded triples

    Example:
        >>> store = load_krown_sample('raw')
    """
    krown_path = Path(__file__).parent / 'KROWN' / 'samples' / sample_name

    if not krown_path.exists():
        raise FileNotFoundError(f"KROWN sample not found: {krown_path}")

    # Find RDF file in sample directory
    rdf_files = list(krown_path.glob('**/*.nt'))

    if not rdf_files:
        # Try other formats
        rdf_files = list(krown_path.glob('**/*.ttl'))
        format = 'turtle'
    else:
        format = 'nt'

    if not rdf_files:
        raise FileNotFoundError(f"No RDF files found in {krown_path}")

    print(f"Loading KROWN sample: {sample_name}")
    return load_rdf_file(str(rdf_files[0]), format=format)


def create_synthetic_dataset() -> PyRDFTripleStore:
    """
    Create small synthetic RDF dataset for testing.

    Returns:
        PyRDFTripleStore with synthetic triples

    Dataset:
        - 5 persons with names and ages
        - 3 friendships between persons
        - Types for all persons
    """
    print("Creating synthetic RDF dataset...")

    g = Graph()

    # Namespaces
    EX = rdflib.Namespace("http://example.org/")
    FOAF = rdflib.Namespace("http://xmlns.com/foaf/0.1/")

    # Add prefixes
    g.bind("ex", EX)
    g.bind("foaf", FOAF)

    # Create 5 persons
    persons = [
        (EX.alice, "Alice", 30),
        (EX.bob, "Bob", 25),
        (EX.charlie, "Charlie", 35),
        (EX.diana, "Diana", 28),
        (EX.eve, "Eve", 32)
    ]

    for person_uri, name, age in persons:
        g.add((person_uri, RDF.type, FOAF.Person))
        g.add((person_uri, FOAF.name, Literal(name)))
        g.add((person_uri, FOAF.age, Literal(age)))

    # Add friendships
    g.add((EX.alice, FOAF.knows, EX.bob))
    g.add((EX.bob, FOAF.knows, EX.charlie))
    g.add((EX.charlie, FOAF.knows, EX.alice))

    print(f"  Created {len(g)} triples")

    # Convert to PyRDFTripleStore
    term_dict = TermDictionary()

    subjects = []
    predicates = []
    objects = []

    for s, p, o in g:
        s_id = term_dict.add_term(s)
        p_id = term_dict.add_term(p)
        o_id = term_dict.add_term(o)

        subjects.append(s_id)
        predicates.append(p_id)
        objects.append(o_id)

    triples_table = pa.Table.from_pydict({
        's': pa.array(subjects, type=pa.int64()),
        'p': pa.array(predicates, type=pa.int64()),
        'o': pa.array(objects, type=pa.int64())
    })

    terms_table = term_dict.to_arrow_table()

    store = PyRDFTripleStore()
    store.load_from_tables(triples_table, terms_table)

    print(f"✅ Created synthetic dataset")

    return store


if __name__ == '__main__':
    # Test the loader with synthetic data
    print("=" * 60)
    print("RDF Loader Test")
    print("=" * 60)

    # Create synthetic dataset
    store = create_synthetic_dataset()

    print(f"\nDataset stats:")
    print(f"  Triples: {store.num_triples()}")
    print(f"  Terms: {store.num_terms()}")

    print("\n✅ RDF loader working correctly!")
