"""
Generate synthetic FOAF (Friend of a Friend) data for RDF/SPARQL benchmarks.

Creates N-Triples format RDF data with:
- Person entities (rdf:type foaf:Person)
- Names (foaf:name)
- Ages (foaf:age)
- Friendship relationships (foaf:knows)

The generated data has realistic properties:
- Configurable number of persons
- Realistic name distribution from a pool of names
- Age distribution (18-80)
- Social graph with power-law friendship distribution
"""

import random
import sys
from pathlib import Path
from typing import List, Set, Tuple

# FOAF and RDF namespaces
RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
FOAF_PERSON = "<http://xmlns.com/foaf/0.1/Person>"
FOAF_NAME = "<http://xmlns.com/foaf/0.1/name>"
FOAF_AGE = "<http://xmlns.com/foaf/0.1/age>"
FOAF_KNOWS = "<http://xmlns.com/foaf/0.1/knows>"

# Name pool for realistic data
FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry",
    "Iris", "Jack", "Kate", "Liam", "Mary", "Noah", "Olivia", "Peter",
    "Quinn", "Rose", "Sam", "Tara", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zack", "Anna", "Ben", "Clara", "Dan", "Emma", "Felix",
    "Gina", "Hugo", "Ivy", "James", "Kim", "Leo", "Mia", "Nick",
    "Oscar", "Pam", "Raj", "Sara", "Tom", "Una", "Vera", "Will",
    "Zoe", "Amy", "Carl", "Dora", "Eric", "Fay", "Greg", "Hope"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz"
]


def generate_person_uri(person_id: int) -> str:
    """Generate unique URI for a person."""
    return f"<http://example.org/person/{person_id}>"


def generate_name() -> str:
    """Generate random realistic name."""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return f'"{first} {last}"'


def generate_age() -> int:
    """Generate age with realistic distribution (18-80, skewed toward 25-45)."""
    # Use triangular distribution for realistic age skew
    return int(random.triangular(18, 80, 35))


def generate_friendship_graph(num_persons: int, avg_friends: int = 10) -> List[Tuple[int, int]]:
    """
    Generate friendship relationships with power-law distribution.

    Creates a social graph where:
    - Most people have around avg_friends connections
    - Some people are highly connected (hubs)
    - Relationships are bidirectional (symmetric)

    Args:
        num_persons: Number of persons in the dataset
        avg_friends: Average number of friends per person

    Returns:
        List of (person_id1, person_id2) friendship pairs
    """
    edges: Set[Tuple[int, int]] = set()

    # Calculate total edges needed
    target_edges = (num_persons * avg_friends) // 2

    # Generate edges with preferential attachment (power-law)
    degrees = [0] * num_persons

    while len(edges) < target_edges:
        # Choose first person uniformly
        person1 = random.randrange(num_persons)

        # Choose second person with preference for low-degree nodes
        # (this creates hubs)
        if random.random() < 0.7:  # 70% preferential
            weights = [1.0 / (degrees[i] + 1) for i in range(num_persons)]
            person2 = random.choices(range(num_persons), weights=weights)[0]
        else:  # 30% uniform (for new connections)
            person2 = random.randrange(num_persons)

        # Avoid self-loops and duplicates
        if person1 != person2:
            edge = tuple(sorted([person1, person2]))
            if edge not in edges:
                edges.add(edge)
                degrees[person1] += 1
                degrees[person2] += 1

    return list(edges)


def generate_foaf_data(
    num_persons: int,
    avg_friends: int = 10,
    output_file: str = "foaf_benchmark.nt",
    seed: int = 42
) -> int:
    """
    Generate FOAF N-Triples data.

    Args:
        num_persons: Number of persons to generate
        avg_friends: Average number of friends per person
        output_file: Output file path
        seed: Random seed for reproducibility

    Returns:
        Total number of triples generated
    """
    random.seed(seed)

    print(f"Generating FOAF data with {num_persons:,} persons...")
    print(f"Target average friends: {avg_friends}")
    print(f"Output file: {output_file}")
    print()

    triple_count = 0

    with open(output_file, 'w') as f:
        # Generate person type, name, and age triples
        print("Generating person attributes...")
        for person_id in range(num_persons):
            person_uri = generate_person_uri(person_id)

            # rdf:type foaf:Person
            f.write(f"{person_uri} {RDF_TYPE} {FOAF_PERSON} .\n")
            triple_count += 1

            # foaf:name
            name = generate_name()
            f.write(f"{person_uri} {FOAF_NAME} {name} .\n")
            triple_count += 1

            # foaf:age
            age = generate_age()
            f.write(f'{person_uri} {FOAF_AGE} "{age}"^^<http://www.w3.org/2001/XMLSchema#integer> .\n')
            triple_count += 1

            if (person_id + 1) % 10000 == 0:
                print(f"  Generated {person_id + 1:,}/{num_persons:,} persons ({triple_count:,} triples)")

        print(f"  Completed {num_persons:,} persons ({triple_count:,} triples)")
        print()

        # Generate friendship relationships
        print("Generating friendship graph...")
        friendships = generate_friendship_graph(num_persons, avg_friends)

        print(f"  Generated {len(friendships):,} friendship pairs")
        print()

        print("Writing friendship triples...")
        for i, (person1, person2) in enumerate(friendships):
            uri1 = generate_person_uri(person1)
            uri2 = generate_person_uri(person2)

            # Both directions (friendship is symmetric)
            f.write(f"{uri1} {FOAF_KNOWS} {uri2} .\n")
            f.write(f"{uri2} {FOAF_KNOWS} {uri1} .\n")
            triple_count += 2

            if (i + 1) % 10000 == 0:
                print(f"  Written {i + 1:,}/{len(friendships):,} friendship pairs ({triple_count:,} triples)")

        print(f"  Completed {len(friendships):,} friendship pairs")
        print()

    print(f"✅ Generated {triple_count:,} triples total")
    print(f"   File: {output_file}")
    print(f"   Size: {Path(output_file).stat().st_size / 1024 / 1024:.1f} MB")

    return triple_count


def main():
    """Main entry point with CLI argument parsing."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate synthetic FOAF data for RDF/SPARQL benchmarks"
    )
    parser.add_argument(
        "--persons",
        type=int,
        default=10000,
        help="Number of persons to generate (default: 10,000)"
    )
    parser.add_argument(
        "--friends",
        type=int,
        default=10,
        help="Average number of friends per person (default: 10)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="foaf_benchmark.nt",
        help="Output file path (default: foaf_benchmark.nt)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )

    args = parser.parse_args()

    # Generate data
    triple_count = generate_foaf_data(
        num_persons=args.persons,
        avg_friends=args.friends,
        output_file=args.output,
        seed=args.seed
    )

    # Print statistics
    print()
    print("Data Statistics:")
    print(f"  Persons: {args.persons:,}")
    print(f"  Expected friendships: ~{args.persons * args.friends:,}")
    print(f"  Total triples: {triple_count:,}")
    print(f"    Type triples: {args.persons:,}")
    print(f"    Name triples: {args.persons:,}")
    print(f"    Age triples: {args.persons:,}")
    print(f"    Knows triples: {triple_count - 3 * args.persons:,}")


if __name__ == "__main__":
    main()
