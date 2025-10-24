"""
RDF Data Loader - N-Triples Parser

Loads RDF data from various formats into Sabot's RDF triple store.

Supported formats:
- N-Triples (.nt, .nt.gz, .nt.xz)
- Turtle (.ttl) - basic support

Features:
- Streaming parser for large files
- Compressed file support (.gz, .xz)
- Progress reporting
- Batch loading for efficiency
- Arrow-native data cleaning and validation
"""

import re
import gzip
import lzma
from pathlib import Path
from typing import List, Tuple, Optional, Iterator
import logging

logger = logging.getLogger(__name__)

# Import Sabot's Arrow modules
try:
    from sabot import cyarrow as pa
    pc = pa.compute  # Arrow compute functions
    ARROW_AVAILABLE = True
except ImportError:
    logger.warning("Sabot Arrow not available, some features disabled")
    ARROW_AVAILABLE = False
    pa = None
    pc = None


class NTriplesParser:
    """
    Parse N-Triples format RDF data.

    N-Triples format:
        <subject> <predicate> <object> .
        <subject> <predicate> "literal" .
        <subject> <predicate> "literal"^^<datatype> .
        <subject> <predicate> "literal"@lang .

    Example:
        parser = NTriplesParser('olympics.nt.xz')
        triples = parser.parse(limit=1000)

        for s, p, o, is_literal, datatype, lang in triples:
            print(f"{s} {p} {o}")
    """

    # Regex patterns for N-Triples parsing
    IRI_PATTERN = re.compile(r'<([^>]+)>')
    LITERAL_PATTERN = re.compile(r'"([^"]*)"(?:\^\^<([^>]+)>|@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*))?')

    def __init__(self, input_path: str):
        """
        Initialize parser.

        Args:
            input_path: Path to N-Triples file (may be compressed)
        """
        self.input_path = Path(input_path)

        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

    def parse(self, limit: Optional[int] = None,
              batch_size: int = 10000,
              show_progress: bool = True) -> List[Tuple]:
        """
        Parse N-Triples file and return list of triples.

        Args:
            limit: Maximum number of triples to parse (None = all)
            batch_size: Print progress every N triples
            show_progress: Whether to print progress messages

        Returns:
            List of tuples: (subject, predicate, object, is_literal, datatype, lang)
        """
        triples = []

        for i, triple in enumerate(self._parse_stream()):
            if limit and i >= limit:
                break

            triples.append(triple)

            if show_progress and (i + 1) % batch_size == 0:
                logger.info(f"Parsed {i + 1:,} triples...")

        if show_progress:
            logger.info(f"Completed: Parsed {len(triples):,} total triples")

        return triples

    def parse_to_arrow(self, limit: Optional[int] = None,
                       batch_size: int = 10000,
                       show_progress: bool = True):
        """
        Parse N-Triples and return Arrow table for cleaning/processing.

        Args:
            limit: Maximum number of triples to parse
            batch_size: Progress reporting interval
            show_progress: Print progress messages

        Returns:
            Arrow Table with columns: subject, predicate, object, is_literal, datatype, lang

        Example:
            parser = NTriplesParser('data.nt.xz')
            table = parser.parse_to_arrow(limit=10000)

            # Clean with Arrow compute
            cleaned = clean_rdf_data(table)

            # Load into store
            store = RDFStore()
            load_arrow_to_store(store, cleaned)
        """
        if not ARROW_AVAILABLE:
            raise RuntimeError("Sabot Arrow not available")

        # Parse triples
        triples = self.parse(limit=limit, batch_size=batch_size, show_progress=show_progress)

        # Convert to Arrow table
        subjects, predicates, objects = [], [], []
        is_literals, datatypes, langs = [], [], []

        for s, p, o, is_lit, dt, lang in triples:
            subjects.append(s)
            predicates.append(p)
            objects.append(o)
            is_literals.append(is_lit)
            datatypes.append(dt)
            langs.append(lang)

        table = pa.table({
            'subject': pa.array(subjects, type=pa.string()),
            'predicate': pa.array(predicates, type=pa.string()),
            'object': pa.array(objects, type=pa.string()),
            'is_literal': pa.array(is_literals, type=pa.bool_()),
            'datatype': pa.array(datatypes, type=pa.string()),
            'lang': pa.array(langs, type=pa.string())
        })

        return table

    def parse_to_store(self, store, limit: Optional[int] = None,
                      batch_size: int = 10000,
                      show_progress: bool = True) -> int:
        """
        Parse N-Triples and load directly into RDF store.

        Args:
            store: RDFStore instance
            limit: Maximum number of triples to load
            batch_size: Load triples in batches of this size
            show_progress: Print progress messages

        Returns:
            Number of triples loaded
        """
        batch = []
        count = 0

        for i, triple in enumerate(self._parse_stream()):
            if limit and i >= limit:
                break

            # Unpack triple
            s, p, o, is_literal, datatype, lang = triple

            # Convert to store format
            batch.append((s, p, o, is_literal, datatype, lang))

            # Batch insert
            if len(batch) >= batch_size:
                store.add_many(batch)
                count += len(batch)

                if show_progress:
                    logger.info(f"Loaded {count:,} triples...")

                batch = []

        # Load remaining triples
        if batch:
            store.add_many(batch)
            count += len(batch)

        if show_progress:
            logger.info(f"Completed: Loaded {count:,} total triples into store")

        return count

    def _parse_stream(self) -> Iterator[Tuple]:
        """
        Stream parse N-Triples file line by line.

        Yields:
            (subject, predicate, object, is_literal, datatype, lang)
        """
        # Determine opener based on file extension
        if self.input_path.suffix == '.xz':
            open_func = lzma.open
        elif self.input_path.suffix == '.gz':
            open_func = gzip.open
        else:
            open_func = open

        with open_func(self.input_path, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue

                # Parse triple
                try:
                    triple = self._parse_line(line)
                    if triple:
                        yield triple
                except Exception as e:
                    logger.warning(f"Line {line_num}: Failed to parse: {e}")
                    logger.debug(f"Problematic line: {line[:100]}")

    def _parse_line(self, line: str) -> Optional[Tuple]:
        """
        Parse a single N-Triple line.

        Args:
            line: N-Triple line (must end with ' .')

        Returns:
            (subject, predicate, object, is_literal, datatype, lang) or None
        """
        # Must end with ' .'
        if not line.endswith(' .'):
            return None

        # Remove trailing ' .'
        line = line[:-2].strip()

        # Extract subject (must be IRI)
        match_s = self.IRI_PATTERN.match(line)
        if not match_s:
            return None

        subject = match_s.group(1)
        line = line[match_s.end():].strip()

        # Extract predicate (must be IRI)
        match_p = self.IRI_PATTERN.match(line)
        if not match_p:
            return None

        predicate = match_p.group(1)
        line = line[match_p.end():].strip()

        # Extract object (IRI or literal)
        is_literal = False
        datatype = ''
        lang = ''

        if line.startswith('<'):
            # Object is IRI
            match_o = self.IRI_PATTERN.match(line)
            if not match_o:
                return None
            obj = match_o.group(1)

        elif line.startswith('"'):
            # Object is literal
            is_literal = True
            match_lit = self.LITERAL_PATTERN.match(line)
            if not match_lit:
                return None

            obj = match_lit.group(1)
            datatype = match_lit.group(2) or ''
            lang = match_lit.group(3) or ''

        else:
            # Unexpected format
            return None

        return (subject, predicate, obj, is_literal, datatype, lang)


def load_ntriples(file_path: str,
                  store=None,
                  limit: Optional[int] = None,
                  batch_size: int = 10000,
                  show_progress: bool = True):
    """
    Convenience function to load N-Triples into RDF store.

    Args:
        file_path: Path to N-Triples file
        store: RDFStore instance (created if None)
        limit: Maximum number of triples to load
        batch_size: Batch size for loading
        show_progress: Show progress messages

    Returns:
        (store, num_triples_loaded)

    Example:
        from sabot.rdf import RDFStore
        from sabot.rdf_loader import load_ntriples

        store, count = load_ntriples('olympics.nt.xz', limit=10000)
        print(f"Loaded {count} triples")
    """
    # Create store if needed
    if store is None:
        from sabot.rdf import RDFStore
        store = RDFStore()

    # Parse and load
    parser = NTriplesParser(file_path)
    count = parser.parse_to_store(
        store,
        limit=limit,
        batch_size=batch_size,
        show_progress=show_progress
    )

    return store, count


# Alias for convenience
parse_ntriples = load_ntriples


def clean_rdf_data(table, remove_duplicates: bool = True,
                   filter_empty: bool = True,
                   normalize_whitespace: bool = True):
    """
    Clean RDF data using Sabot's Arrow compute operators.

    Args:
        table: Arrow table with RDF triples
        remove_duplicates: Remove duplicate triples
        filter_empty: Remove triples with empty subjects/predicates/objects
        normalize_whitespace: Trim whitespace from literals

    Returns:
        Cleaned Arrow table

    Example:
        parser = NTriplesParser('data.nt.xz')
        raw_data = parser.parse_to_arrow(limit=10000)

        # Clean using Arrow compute
        clean_data = clean_rdf_data(raw_data)

        # Load into store
        store = RDFStore()
        load_arrow_to_store(store, clean_data)
    """
    if not ARROW_AVAILABLE:
        raise RuntimeError("Sabot Arrow not available")

    logger.info(f"Cleaning RDF data: {table.num_rows:,} triples")

    # Filter empty values using Arrow compute
    if filter_empty:
        # Create masks for non-empty values
        subject_mask = pc.not_equal(pc.utf8_length(table.column('subject')), 0)
        predicate_mask = pc.not_equal(pc.utf8_length(table.column('predicate')), 0)
        object_mask = pc.not_equal(pc.utf8_length(table.column('object')), 0)

        # Combine masks
        valid_mask = pc.and_(pc.and_(subject_mask, predicate_mask), object_mask)

        # Filter table
        table = table.filter(valid_mask)
        logger.info(f"  After filtering empty: {table.num_rows:,} triples")

    # Normalize whitespace in literals using Arrow compute
    if normalize_whitespace:
        # Trim whitespace from all string columns
        subject_trimmed = pc.utf8_trim(table.column('subject'), characters=' \t\n\r')
        predicate_trimmed = pc.utf8_trim(table.column('predicate'), characters=' \t\n\r')
        object_trimmed = pc.utf8_trim(table.column('object'), characters=' \t\n\r')

        # Rebuild table with trimmed values
        table = pa.table({
            'subject': subject_trimmed,
            'predicate': predicate_trimmed,
            'object': object_trimmed,
            'is_literal': table.column('is_literal'),
            'datatype': table.column('datatype'),
            'lang': table.column('lang')
        })
        logger.info(f"  After whitespace normalization: {table.num_rows:,} triples")

    # Remove duplicates using Arrow compute
    if remove_duplicates:
        # Create composite key for deduplication
        # Concatenate subject + predicate + object
        composite_key = pc.binary_join_element_wise(
            pc.binary_join_element_wise(
                table.column('subject'),
                table.column('predicate'),
                '|'
            ),
            table.column('object'),
            '|'
        )

        # Get unique indices
        unique_keys = pc.unique(composite_key)

        # Create mask for first occurrence of each triple
        seen = set()
        keep_mask = []

        for i in range(table.num_rows):
            key = composite_key[i].as_py()
            if key not in seen:
                seen.add(key)
                keep_mask.append(True)
            else:
                keep_mask.append(False)

        # Filter to unique triples
        table = table.filter(pa.array(keep_mask))
        logger.info(f"  After deduplication: {table.num_rows:,} triples")

    logger.info(f"✓ Cleaning complete: {table.num_rows:,} clean triples")
    return table


def filter_by_predicate(table, predicates: List[str]):
    """
    Filter RDF triples by predicate URIs using Arrow compute.

    Args:
        table: Arrow table with RDF triples
        predicates: List of predicate URIs to keep

    Returns:
        Filtered Arrow table

    Example:
        # Keep only type and label triples
        filtered = filter_by_predicate(table, [
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
            'http://www.w3.org/2000/01/rdf-schema#label'
        ])
    """
    if not ARROW_AVAILABLE:
        raise RuntimeError("Sabot Arrow not available")

    predicate_col = table.column('predicate')

    # Create mask using Arrow compute
    mask = pa.array([False] * table.num_rows)

    for pred in predicates:
        pred_mask = pc.equal(predicate_col, pred)
        mask = pc.or_(mask, pred_mask)

    filtered = table.filter(mask)
    logger.info(f"Filtered by predicate: {table.num_rows:,} → {filtered.num_rows:,} triples")

    return filtered


def filter_literals_by_datatype(table, datatypes: List[str]):
    """
    Filter literal triples by datatype using Arrow compute.

    Args:
        table: Arrow table with RDF triples
        datatypes: List of XSD datatype URIs to keep

    Returns:
        Filtered Arrow table

    Example:
        # Keep only integer and double literals
        filtered = filter_literals_by_datatype(table, [
            'http://www.w3.org/2001/XMLSchema#integer',
            'http://www.w3.org/2001/XMLSchema#double'
        ])
    """
    if not ARROW_AVAILABLE:
        raise RuntimeError("Sabot Arrow not available")

    # Filter to literals only
    literal_mask = table.column('is_literal')
    datatype_col = table.column('datatype')

    # Create mask for desired datatypes
    type_mask = pa.array([False] * table.num_rows)

    for dtype in datatypes:
        dtype_mask = pc.equal(datatype_col, dtype)
        type_mask = pc.or_(type_mask, dtype_mask)

    # Combine masks
    final_mask = pc.and_(literal_mask, type_mask)

    filtered = table.filter(final_mask)
    logger.info(f"Filtered by datatype: {table.num_rows:,} → {filtered.num_rows:,} triples")

    return filtered


def load_arrow_to_store(store, table):
    """
    Load Arrow table of cleaned triples into RDF store.

    Args:
        store: RDFStore instance
        table: Arrow table with columns: subject, predicate, object, is_literal, datatype, lang

    Returns:
        Number of triples loaded

    Example:
        # Parse and clean
        parser = NTriplesParser('data.nt.xz')
        raw_data = parser.parse_to_arrow(limit=10000)
        clean_data = clean_rdf_data(raw_data)

        # Load into store
        store = RDFStore()
        count = load_arrow_to_store(store, clean_data)
    """
    # Convert Arrow table back to tuples for RDFStore
    triples = []

    for i in range(table.num_rows):
        s = table.column('subject')[i].as_py()
        p = table.column('predicate')[i].as_py()
        o = table.column('object')[i].as_py()
        is_lit = table.column('is_literal')[i].as_py()
        dtype = table.column('datatype')[i].as_py() or ''
        lang = table.column('lang')[i].as_py() or ''

        triples.append((s, p, o, is_lit, dtype, lang))

    # Batch load
    store.add_many(triples)

    logger.info(f"Loaded {len(triples):,} triples into RDF store")
    return len(triples)
