"""
Load Kuzu social network dataset into Sabot's GraphQueryEngine.

Converts Polars/Parquet data to Arrow tables for zero-copy loading.
"""
import polars as pl
from pathlib import Path

# Use Sabot's vendored Arrow
from sabot import cyarrow as ca


def load_nodes_persons(data_dir: Path) -> ca.Table:
    """Load Person nodes."""
    persons_path = data_dir / "nodes" / "persons.parquet"
    df = pl.read_parquet(persons_path)

    # Convert to Arrow
    return df.to_arrow()


def load_nodes_cities(data_dir: Path) -> ca.Table:
    """Load City nodes."""
    cities_path = data_dir / "nodes" / "cities.parquet"
    df = pl.read_parquet(cities_path)
    return df.to_arrow()


def load_nodes_states(data_dir: Path) -> ca.Table:
    """Load State nodes."""
    states_path = data_dir / "nodes" / "states.parquet"
    df = pl.read_parquet(states_path)
    return df.to_arrow()


def load_nodes_countries(data_dir: Path) -> ca.Table:
    """Load Country nodes."""
    countries_path = data_dir / "nodes" / "countries.parquet"
    df = pl.read_parquet(countries_path)
    return df.to_arrow()


def load_nodes_interests(data_dir: Path) -> ca.Table:
    """Load Interest nodes."""
    interests_path = data_dir / "nodes" / "interests.parquet"
    df = pl.read_parquet(interests_path)
    return df.to_arrow()


def load_edges_follows(data_dir: Path) -> ca.Table:
    """Load Follows edges."""
    follows_path = data_dir / "edges" / "follows.parquet"
    df = pl.read_parquet(follows_path)

    # Rename columns to match Sabot conventions (source, target)
    df = df.rename({"from": "source", "to": "target"})
    return df.to_arrow()


def load_edges_lives_in(data_dir: Path) -> ca.Table:
    """Load LivesIn edges."""
    lives_in_path = data_dir / "edges" / "lives_in.parquet"
    df = pl.read_parquet(lives_in_path)
    df = df.rename({"from": "source", "to": "target"})
    return df.to_arrow()


def load_edges_interested_in(data_dir: Path) -> ca.Table:
    """Load HasInterest edges."""
    interested_path = data_dir / "edges" / "interested_in.parquet"
    df = pl.read_parquet(interested_path)
    df = df.rename({"from": "source", "to": "target"})
    return df.to_arrow()


def load_edges_city_in(data_dir: Path) -> ca.Table:
    """Load CityIn edges."""
    city_in_path = data_dir / "edges" / "city_in.parquet"
    df = pl.read_parquet(city_in_path)
    df = df.rename({"from": "source", "to": "target"})
    return df.to_arrow()


def load_edges_state_in(data_dir: Path) -> ca.Table:
    """Load StateIn edges."""
    state_in_path = data_dir / "edges" / "state_in.parquet"
    df = pl.read_parquet(state_in_path)
    df = df.rename({"from": "source", "to": "target"})
    return df.to_arrow()


def load_all_data(data_dir: Path) -> dict[str, ca.Table]:
    """Load all nodes and edges from dataset.

    Returns:
        Dictionary with keys:
            - person_nodes
            - city_nodes
            - state_nodes
            - country_nodes
            - interest_nodes
            - follows_edges
            - lives_in_edges
            - has_interest_edges
            - city_in_edges
            - state_in_edges
    """
    print("Loading social network dataset...")

    data = {
        # Nodes
        "person_nodes": load_nodes_persons(data_dir),
        "city_nodes": load_nodes_cities(data_dir),
        "state_nodes": load_nodes_states(data_dir),
        "country_nodes": load_nodes_countries(data_dir),
        "interest_nodes": load_nodes_interests(data_dir),

        # Edges
        "follows_edges": load_edges_follows(data_dir),
        "lives_in_edges": load_edges_lives_in(data_dir),
        "has_interest_edges": load_edges_interested_in(data_dir),
        "city_in_edges": load_edges_city_in(data_dir),
        "state_in_edges": load_edges_state_in(data_dir),
    }

    # Print summary
    print("\nDataset loaded:")
    print(f"  Persons: {data['person_nodes'].num_rows:,}")
    print(f"  Cities: {data['city_nodes'].num_rows:,}")
    print(f"  States: {data['state_nodes'].num_rows:,}")
    print(f"  Countries: {data['country_nodes'].num_rows:,}")
    print(f"  Interests: {data['interest_nodes'].num_rows:,}")
    print(f"  Follows edges: {data['follows_edges'].num_rows:,}")
    print(f"  LivesIn edges: {data['lives_in_edges'].num_rows:,}")
    print(f"  HasInterest edges: {data['has_interest_edges'].num_rows:,}")
    print(f"  CityIn edges: {data['city_in_edges'].num_rows:,}")
    print(f"  StateIn edges: {data['state_in_edges'].num_rows:,}")

    total_edges = (
        data['follows_edges'].num_rows +
        data['lives_in_edges'].num_rows +
        data['has_interest_edges'].num_rows +
        data['city_in_edges'].num_rows +
        data['state_in_edges'].num_rows
    )
    print(f"\nTotal edges: {total_edges:,}")

    return data


if __name__ == "__main__":
    # Test loading
    data_dir = Path("/Users/bengamble/Sabot/benchmarks/kuzu_study/reference/data/output")
    data = load_all_data(data_dir)

    print("\n✅ Data loading test successful!")
