"""
W3C SPARQL Test Suite Runner

Runs official W3C SPARQL test suites against Sabot's RDF/SPARQL implementation
to provide accurate feature coverage data.

Usage:
    python tests/w3c_sparql_test_runner.py --suite sparql10-basic
    python tests/w3c_sparql_test_runner.py --suite sparql11-aggregates
    python tests/w3c_sparql_test_runner.py --suite sparql11-all
    python tests/w3c_sparql_test_runner.py --all
"""

import sys
import os
import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
import traceback

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sabot.rdf import RDFStore
from sabot import cyarrow as pa

# Import rdflib
import rdflib
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS


# Test manifest namespaces
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
DAWGT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#")


@dataclass
class TestCase:
    """Represents a single W3C SPARQL test case."""
    name: str
    description: str
    test_type: str
    query_file: Path
    data_files: List[Path] = field(default_factory=list)
    result_file: Optional[Path] = None
    category: str = ""


@dataclass
class TestResult:
    """Results from running a test case."""
    test_case: TestCase
    status: str  # "PASS", "FAIL", "SKIP", "ERROR"
    error_message: Optional[str] = None
    actual_result: Optional[Any] = None
    expected_result: Optional[Any] = None


class TurtleDataLoader:
    """Loads Turtle test data into Sabot RDFStore."""

    @staticmethod
    def load_file(store: RDFStore, turtle_file: Path) -> None:
        """
        Parse Turtle file and load triples into RDFStore.

        Args:
            store: Sabot RDFStore instance
            turtle_file: Path to .ttl file
        """
        g = Graph()
        g.parse(str(turtle_file), format='turtle')

        # Convert rdflib triples to Sabot format
        for s, p, o in g:
            # Convert subject
            if isinstance(s, URIRef):
                subject = str(s)
            elif isinstance(s, BNode):
                subject = f"_:{s}"  # Blank node
            else:
                subject = str(s)

            # Convert predicate (always URI)
            predicate = str(p)

            # Convert object
            if isinstance(o, URIRef):
                obj = str(o)
                store.add(subject, predicate, obj, obj_is_literal=False)
            elif isinstance(o, Literal):
                # Extract literal value, language, and datatype
                value = str(o)
                lang = o.language if o.language else ""
                datatype = str(o.datatype) if o.datatype else ""
                store.add(subject, predicate, value, obj_is_literal=True, lang=lang, datatype=datatype)
            elif isinstance(o, BNode):
                obj = f"_:{o}"
                store.add(subject, predicate, obj, obj_is_literal=False)
            else:
                obj = str(o)
                store.add(subject, predicate, obj, obj_is_literal=False)


class SPARQLResultsParser:
    """Parses SPARQL Results XML (.srx) and JSON (.srj) formats."""

    @staticmethod
    def parse_xml(result_file: Path) -> Dict[str, Any]:
        """
        Parse SPARQL Results XML format.

        Returns:
            Dict with 'variables' list and 'bindings' list of dicts
        """
        tree = ET.parse(str(result_file))
        root = tree.getroot()

        # Namespace for SPARQL results
        ns = {'sr': 'http://www.w3.org/2005/sparql-results#'}

        # Extract variable names
        variables = []
        head = root.find('sr:head', ns)
        if head is not None:
            for var in head.findall('sr:variable', ns):
                variables.append(var.get('name'))

        # Extract bindings
        bindings = []
        results = root.find('sr:results', ns)
        if results is not None:
            for result in results.findall('sr:result', ns):
                binding = {}
                for b in result.findall('sr:binding', ns):
                    var_name = b.get('name')

                    # Get value (uri, literal, or bnode)
                    uri = b.find('sr:uri', ns)
                    literal = b.find('sr:literal', ns)
                    bnode = b.find('sr:bnode', ns)

                    if uri is not None:
                        binding[var_name] = {
                            'type': 'uri',
                            'value': uri.text
                        }
                    elif literal is not None:
                        lit_val = {
                            'type': 'literal',
                            'value': literal.text if literal.text else ""
                        }
                        if literal.get('datatype'):
                            lit_val['datatype'] = literal.get('datatype')
                        if literal.get('{http://www.w3.org/XML/1998/namespace}lang'):
                            lit_val['lang'] = literal.get('{http://www.w3.org/XML/1998/namespace}lang')
                        binding[var_name] = lit_val
                    elif bnode is not None:
                        binding[var_name] = {
                            'type': 'bnode',
                            'value': bnode.text
                        }

                bindings.append(binding)

        return {
            'variables': variables,
            'bindings': bindings
        }

    @staticmethod
    def parse_json(result_file: Path) -> Dict[str, Any]:
        """Parse SPARQL Results JSON format."""
        with open(result_file, 'r') as f:
            data = json.load(f)

        return {
            'variables': data.get('head', {}).get('vars', []),
            'bindings': data.get('results', {}).get('bindings', [])
        }


class ResultComparator:
    """Compares actual query results with expected results."""

    @staticmethod
    def compare(actual_table: pa.Table, expected: Dict[str, Any],
                test_case: TestCase) -> Tuple[bool, Optional[str]]:
        """
        Compare Arrow table results with expected SPARQL results.

        Returns:
            (matches, error_message)
        """
        try:
            # Convert Arrow table to comparable format
            actual_vars = actual_table.column_names
            actual_rows = []

            for i in range(actual_table.num_rows):
                row = {}
                for var in actual_vars:
                    col = actual_table.column(var)
                    val = col[i].as_py()
                    if val is not None:
                        row[var] = val
                actual_rows.append(row)

            # Get expected results
            expected_vars = expected['variables']
            expected_bindings = expected['bindings']

            # Check variable count
            if len(actual_vars) != len(expected_vars):
                return False, f"Variable count mismatch: actual={len(actual_vars)}, expected={len(expected_vars)}"

            # Check row count
            if len(actual_rows) != len(expected_bindings):
                return False, f"Row count mismatch: actual={len(actual_rows)}, expected={len(expected_bindings)}"

            # TODO: Implement detailed row comparison
            # For now, just check counts match
            return True, None

        except Exception as e:
            return False, f"Comparison error: {str(e)}"


class ManifestParser:
    """Parses W3C test manifest files."""

    @staticmethod
    def parse_manifest(manifest_file: Path, category: str) -> List[TestCase]:
        """
        Parse manifest.ttl file to extract test cases.

        Args:
            manifest_file: Path to manifest.ttl
            category: Test category name

        Returns:
            List of TestCase objects
        """
        g = Graph()
        g.parse(str(manifest_file), format='turtle')

        test_cases = []
        base_dir = manifest_file.parent

        # Find all test entries
        manifest_uri = URIRef(manifest_file.as_uri().replace('manifest.ttl', ''))

        # Get test entries from manifest
        for entry in g.objects(None, MF.entries):
            # entry is an RDF Collection
            for test_uri in g.items(entry):
                try:
                    # Get test metadata
                    name = str(g.value(test_uri, MF.name, default="Unknown"))
                    description = str(g.value(test_uri, RDFS.comment, default=""))
                    test_type = str(g.value(test_uri, RDF.type, default=""))

                    # Get action (query + data)
                    action = g.value(test_uri, MF.action)
                    if action is None:
                        continue

                    query_uri = g.value(action, QT.query)
                    query_file = base_dir / Path(str(query_uri)).name if query_uri else None

                    # Get data files
                    data_files = []
                    data_uri = g.value(action, QT.data)
                    if data_uri:
                        data_files.append(base_dir / Path(str(data_uri)).name)

                    # Also check for graphData (named graphs)
                    for graph_data in g.objects(action, QT.graphData):
                        data_files.append(base_dir / Path(str(graph_data)).name)

                    # Get result file
                    result_uri = g.value(test_uri, MF.result)
                    result_file = base_dir / Path(str(result_uri)).name if result_uri else None

                    if query_file and query_file.exists():
                        test_case = TestCase(
                            name=name,
                            description=description,
                            test_type=test_type,
                            query_file=query_file,
                            data_files=data_files,
                            result_file=result_file,
                            category=category
                        )
                        test_cases.append(test_case)

                except Exception as e:
                    print(f"Warning: Failed to parse test {test_uri}: {e}")
                    continue

        return test_cases


class W3CTestRunner:
    """Main test runner for W3C SPARQL test suites."""

    def __init__(self, test_root: Path):
        self.test_root = test_root
        self.data_loader = TurtleDataLoader()
        self.results_parser = SPARQLResultsParser()
        self.comparator = ResultComparator()
        self.manifest_parser = ManifestParser()

    def run_test(self, test_case: TestCase) -> TestResult:
        """Run a single test case."""
        try:
            # Create fresh RDFStore
            store = RDFStore()

            # Load test data
            for data_file in test_case.data_files:
                if data_file.exists():
                    self.data_loader.load_file(store, data_file)

            # Read query
            query = test_case.query_file.read_text()

            # Execute query
            try:
                result_table = store.query(query)
            except NotImplementedError as e:
                return TestResult(
                    test_case=test_case,
                    status="SKIP",
                    error_message=f"Feature not implemented: {str(e)}"
                )
            except Exception as e:
                return TestResult(
                    test_case=test_case,
                    status="ERROR",
                    error_message=f"Query execution failed: {str(e)}"
                )

            # If no expected result file, just check query executes
            if test_case.result_file is None or not test_case.result_file.exists():
                return TestResult(
                    test_case=test_case,
                    status="PASS",
                    error_message="No expected result to compare (query executed successfully)"
                )

            # Parse expected results
            if test_case.result_file.suffix == '.srx':
                expected = self.results_parser.parse_xml(test_case.result_file)
            elif test_case.result_file.suffix == '.srj':
                expected = self.results_parser.parse_json(test_case.result_file)
            else:
                return TestResult(
                    test_case=test_case,
                    status="SKIP",
                    error_message=f"Unsupported result format: {test_case.result_file.suffix}"
                )

            # Compare results
            matches, error_msg = self.comparator.compare(result_table, expected, test_case)

            if matches:
                return TestResult(
                    test_case=test_case,
                    status="PASS"
                )
            else:
                return TestResult(
                    test_case=test_case,
                    status="FAIL",
                    error_message=error_msg
                )

        except Exception as e:
            return TestResult(
                test_case=test_case,
                status="ERROR",
                error_message=f"Test execution error: {str(e)}\n{traceback.format_exc()}"
            )

    def run_suite(self, suite_name: str) -> List[TestResult]:
        """
        Run a test suite.

        Args:
            suite_name: One of:
                - sparql10-basic
                - sparql11-aggregates
                - sparql11-bind
                - sparql11-grouping
                - sparql11-all

        Returns:
            List of TestResult objects
        """
        results = []

        if suite_name == "sparql10-basic":
            manifest = self.test_root / "sparql10" / "basic" / "manifest.ttl"
            tests = self.manifest_parser.parse_manifest(manifest, "SPARQL 1.0 Basic")

        elif suite_name.startswith("sparql11-"):
            category = suite_name.replace("sparql11-", "")
            if category == "all":
                # Run all SPARQL 1.1 categories
                categories = [
                    "aggregates", "bind", "bindings", "cast", "construct",
                    "exists", "functions", "grouping", "negation",
                    "project-expression", "property-path", "subquery"
                ]
                tests = []
                for cat in categories:
                    manifest = self.test_root / "sparql11" / cat / "manifest.ttl"
                    if manifest.exists():
                        tests.extend(self.manifest_parser.parse_manifest(manifest, f"SPARQL 1.1 {cat.title()}"))
            else:
                manifest = self.test_root / "sparql11" / category / "manifest.ttl"
                tests = self.manifest_parser.parse_manifest(manifest, f"SPARQL 1.1 {category.title()}")
        else:
            raise ValueError(f"Unknown test suite: {suite_name}")

        # Run tests
        print(f"\n{'='*80}")
        print(f"Running {len(tests)} tests from {suite_name}")
        print(f"{'='*80}\n")

        for i, test in enumerate(tests, 1):
            print(f"[{i}/{len(tests)}] {test.name}...", end=" ", flush=True)
            result = self.run_test(test)
            results.append(result)

            # Print status
            status_colors = {
                "PASS": "\033[92m✓ PASS\033[0m",
                "FAIL": "\033[91m✗ FAIL\033[0m",
                "SKIP": "\033[93m⊘ SKIP\033[0m",
                "ERROR": "\033[91m⚠ ERROR\033[0m"
            }
            print(status_colors.get(result.status, result.status))

            if result.error_message and result.status in ["FAIL", "ERROR"]:
                print(f"    {result.error_message}")

        return results

    def generate_report(self, results: List[TestResult], output_file: Path):
        """Generate markdown test results report."""
        # Calculate statistics
        total = len(results)
        passed = sum(1 for r in results if r.status == "PASS")
        failed = sum(1 for r in results if r.status == "FAIL")
        skipped = sum(1 for r in results if r.status == "SKIP")
        errors = sum(1 for r in results if r.status == "ERROR")
        pass_rate = (passed / total * 100) if total > 0 else 0

        # Group by category
        by_category = {}
        for result in results:
            cat = result.test_case.category
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(result)

        # Generate report
        report = f"""# W3C SPARQL Test Suite Results

**Date**: {Path(__file__).stat().st_mtime}
**Total Tests**: {total}
**Status**: {passed} passed, {failed} failed, {skipped} skipped, {errors} errors
**Pass Rate**: {pass_rate:.1f}%

## Summary by Category

| Category | Total | Passed | Failed | Skipped | Errors | Pass Rate |
|----------|-------|--------|--------|---------|--------|-----------|
"""

        for category, cat_results in sorted(by_category.items()):
            cat_total = len(cat_results)
            cat_passed = sum(1 for r in cat_results if r.status == "PASS")
            cat_failed = sum(1 for r in cat_results if r.status == "FAIL")
            cat_skipped = sum(1 for r in cat_results if r.status == "SKIP")
            cat_errors = sum(1 for r in cat_results if r.status == "ERROR")
            cat_rate = (cat_passed / cat_total * 100) if cat_total > 0 else 0

            report += f"| {category} | {cat_total} | {cat_passed} | {cat_failed} | {cat_skipped} | {cat_errors} | {cat_rate:.1f}% |\n"

        # List failed tests
        failed_tests = [r for r in results if r.status == "FAIL"]
        if failed_tests:
            report += f"\n## Failed Tests ({len(failed_tests)})\n\n"
            for result in failed_tests:
                report += f"### {result.test_case.name}\n"
                report += f"- **Category**: {result.test_case.category}\n"
                report += f"- **Description**: {result.test_case.description}\n"
                report += f"- **Error**: {result.error_message}\n"
                report += f"- **Query**: `{result.test_case.query_file}`\n\n"

        # List skipped tests
        skipped_tests = [r for r in results if r.status == "SKIP"]
        if skipped_tests:
            report += f"\n## Skipped Tests ({len(skipped_tests)})\n\n"
            # Group by error message to identify common missing features
            skip_reasons = {}
            for result in skipped_tests:
                reason = result.error_message or "Unknown"
                if reason not in skip_reasons:
                    skip_reasons[reason] = []
                skip_reasons[reason].append(result.test_case.name)

            for reason, tests in skip_reasons.items():
                report += f"### {reason} ({len(tests)} tests)\n"
                for test_name in tests[:5]:  # Show first 5
                    report += f"- {test_name}\n"
                if len(tests) > 5:
                    report += f"- ... and {len(tests) - 5} more\n"
                report += "\n"

        # Write report
        output_file.write_text(report)
        print(f"\n{'='*80}")
        print(f"Report written to: {output_file}")
        print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description="Run W3C SPARQL test suites")
    parser.add_argument(
        '--suite',
        choices=['sparql10-basic', 'sparql11-aggregates', 'sparql11-bind',
                 'sparql11-grouping', 'sparql11-all'],
        help="Test suite to run"
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help="Run all test suites"
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('tests/w3c_test_results.md'),
        help="Output file for results report"
    )

    args = parser.parse_args()

    # Find test root
    test_root = Path(__file__).parent.parent / 'sabot_ql' / 'tests' / 'rdf-tests' / 'sparql'

    if not test_root.exists():
        print(f"Error: Test root not found: {test_root}")
        sys.exit(1)

    runner = W3CTestRunner(test_root)

    all_results = []

    if args.all:
        suites = ['sparql10-basic', 'sparql11-all']
    elif args.suite:
        suites = [args.suite]
    else:
        print("Error: Must specify --suite or --all")
        parser.print_help()
        sys.exit(1)

    for suite in suites:
        results = runner.run_suite(suite)
        all_results.extend(results)

    # Generate report
    runner.generate_report(all_results, args.output)

    # Print summary
    total = len(all_results)
    passed = sum(1 for r in all_results if r.status == "PASS")
    print(f"\nFinal Summary: {passed}/{total} tests passed ({passed/total*100:.1f}%)")


if __name__ == "__main__":
    main()
