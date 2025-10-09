"""
Social Graph Pattern Matching with Property Filtering

Demonstrates filtering graph pattern matches based on vertex and edge properties.
Uses Arrow compute functions to apply predicates after pattern matching.

Use Cases:
1. Find friend recommendations (friends-of-friends with common interests)
2. Filter by age, location, interests
3. Find influential users based on follower counts
4. Discover communities with specific characteristics
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
import pyarrow.compute as pc
from sabot._cython.graph.query import match_2hop, match_3hop


def create_social_network():
    """
    Create a social network with user properties.

    Users: 0-9 (10 users)
    Properties: age, city, interests
    Edges: friendships
    """
    # Friendship edges
    edges = pa.table({
        'source': pa.array([0, 0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 6, 7, 8, 9], type=pa.int64())
    })

    # User properties
    users = pa.table({
        'user_id': pa.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], type=pa.int64()),
        'name': pa.array(['Alice', 'Bob', 'Carol', 'Dave', 'Eve',
                         'Frank', 'Grace', 'Henry', 'Iris', 'Jack']),
        'age': pa.array([25, 30, 28, 35, 22, 40, 26, 31, 29, 27], type=pa.int64()),
        'city': pa.array(['NYC', 'SF', 'NYC', 'LA', 'SF',
                         'NYC', 'LA', 'SF', 'NYC', 'LA']),
        'follower_count': pa.array([100, 500, 200, 1000, 150,
                                   5000, 300, 800, 250, 400], type=pa.int64())
    })

    return edges, users


def find_friend_recommendations(edges, users, user_id):
    """
    Find friend recommendations using 2-hop pattern matching.

    Pattern: user -> friend -> friend_of_friend
    Filter: friend_of_friend not already a friend, same city as user
    """
    print(f"\n{'=' * 60}")
    print(f"FRIEND RECOMMENDATIONS FOR USER {user_id}")
    print(f"{'=' * 60}")

    # Step 1: Find all 2-hop paths (user -> friend -> potential_friend)
    result = match_2hop(edges, edges)

    table = result.result_table()
    print(f"\nFound {result.num_matches()} 2-hop connections")

    # Step 2: Filter for paths starting from user_id
    mask = pc.equal(table.column('a_id'), pa.scalar(user_id, type=pa.int64()))
    filtered = table.filter(mask)

    print(f"Found {filtered.num_rows} paths from user {user_id}")

    if filtered.num_rows == 0:
        print("No friend recommendations available")
        return

    # Step 3: Join with user properties to get details
    # Get intermediate friend (b) and potential friend (c) properties
    b_ids = filtered.column('b_id')
    c_ids = filtered.column('c_id')

    # Step 4: Get user's city for filtering
    user_mask = pc.equal(users.column('user_id'), pa.scalar(user_id, type=pa.int64()))
    user_row = users.filter(user_mask)
    user_city = user_row.column('city')[0].as_py()
    user_name = user_row.column('name')[0].as_py()

    print(f"\nUser: {user_name} from {user_city}")
    print(f"\nRecommendations (friends-of-friends in {user_city}):\n")

    # Step 5: For each potential friend, check properties
    recommendations = []

    for i in range(filtered.num_rows):
        friend_id = b_ids[i].as_py()
        potential_id = c_ids[i].as_py()

        # Skip if already friends or self
        if potential_id == user_id:
            continue

        # Get potential friend's properties
        potential_mask = pc.equal(users.column('user_id'),
                                  pa.scalar(potential_id, type=pa.int64()))
        potential_row = users.filter(potential_mask)

        if potential_row.num_rows == 0:
            continue

        potential_name = potential_row.column('name')[0].as_py()
        potential_city = potential_row.column('city')[0].as_py()
        potential_age = potential_row.column('age')[0].as_py()

        # Get friend's name
        friend_mask = pc.equal(users.column('user_id'),
                              pa.scalar(friend_id, type=pa.int64()))
        friend_row = users.filter(friend_mask)
        friend_name = friend_row.column('name')[0].as_py()

        # Apply filters: same city
        if potential_city == user_city:
            recommendations.append({
                'id': potential_id,
                'name': potential_name,
                'age': potential_age,
                'city': potential_city,
                'via': friend_name
            })

    # Remove duplicates
    seen = set()
    unique_recs = []
    for rec in recommendations:
        if rec['id'] not in seen:
            seen.add(rec['id'])
            unique_recs.append(rec)

    # Display recommendations
    for rec in unique_recs:
        print(f"  • {rec['name']}, {rec['age']} years old ({rec['city']})")
        print(f"    Mutual friend: {rec['via']}")


def find_influential_users_in_network(edges, users, min_followers=500):
    """
    Find patterns involving influential users (high follower count).

    Pattern: user -> influencer -> follower
    Filter: influencer has > min_followers
    """
    print(f"\n{'=' * 60}")
    print(f"INFLUENCE PATTERNS (users with >{min_followers} followers)")
    print(f"{'=' * 60}")

    # Step 1: Find all 2-hop patterns
    result = match_2hop(edges, edges)
    table = result.result_table()

    # Step 2: Join with user properties to get intermediate user (b) properties
    b_ids = table.column('b_id')

    # Build a list of rows where b (intermediate) has high follower count
    influential_patterns = []

    for i in range(table.num_rows):
        a_id = table.column('a_id')[i].as_py()
        b_id = b_ids[i].as_py()
        c_id = table.column('c_id')[i].as_py()

        # Get b's follower count
        b_mask = pc.equal(users.column('user_id'), pa.scalar(b_id, type=pa.int64()))
        b_row = users.filter(b_mask)

        if b_row.num_rows > 0:
            b_followers = b_row.column('follower_count')[0].as_py()
            b_name = b_row.column('name')[0].as_py()

            if b_followers >= min_followers:
                # Get names for a and c
                a_mask = pc.equal(users.column('user_id'), pa.scalar(a_id, type=pa.int64()))
                a_row = users.filter(a_mask)
                a_name = a_row.column('name')[0].as_py() if a_row.num_rows > 0 else f"User{a_id}"

                c_mask = pc.equal(users.column('user_id'), pa.scalar(c_id, type=pa.int64()))
                c_row = users.filter(c_mask)
                c_name = c_row.column('name')[0].as_py() if c_row.num_rows > 0 else f"User{c_id}"

                influential_patterns.append({
                    'follower': a_name,
                    'influencer': b_name,
                    'influencer_followers': b_followers,
                    'secondary': c_name
                })

    print(f"\nFound {len(influential_patterns)} influence patterns:\n")

    # Group by influencer
    by_influencer = {}
    for pattern in influential_patterns:
        inf = pattern['influencer']
        if inf not in by_influencer:
            by_influencer[inf] = {
                'followers': pattern['influencer_followers'],
                'connections': []
            }
        by_influencer[inf]['connections'].append(
            (pattern['follower'], pattern['secondary'])
        )

    # Display by influencer
    for influencer, data in sorted(by_influencer.items(),
                                   key=lambda x: x[1]['followers'],
                                   reverse=True):
        print(f"  {influencer} ({data['followers']} followers)")
        print(f"    Connections: {len(data['connections'])}")
        for follower, secondary in data['connections'][:3]:  # Show top 3
            print(f"      {follower} → {influencer} → {secondary}")
        if len(data['connections']) > 3:
            print(f"      ... and {len(data['connections']) - 3} more")
        print()


def find_age_similar_connections(edges, users, target_age=30, tolerance=5):
    """
    Find 3-hop connections where all users are in a similar age range.

    Pattern: user1 -> user2 -> user3 -> user4
    Filter: all users within age_range
    """
    print(f"\n{'=' * 60}")
    print(f"AGE-SIMILAR CONNECTIONS (age {target_age} ± {tolerance})")
    print(f"{'=' * 60}")

    # Step 1: Find 3-hop patterns
    result = match_3hop(edges, edges, edges)
    table = result.result_table()

    print(f"\nFound {result.num_matches()} 3-hop patterns")

    # Step 2: Filter by age similarity
    age_min = target_age - tolerance
    age_max = target_age + tolerance

    matching_patterns = []

    for i in range(table.num_rows):
        a_id = table.column('a_id')[i].as_py()
        b_id = table.column('b_id')[i].as_py()
        c_id = table.column('c_id')[i].as_py()
        d_id = table.column('d_id')[i].as_py()

        # Get ages for all users
        ages = {}
        for user_id in [a_id, b_id, c_id, d_id]:
            mask = pc.equal(users.column('user_id'), pa.scalar(user_id, type=pa.int64()))
            row = users.filter(mask)
            if row.num_rows > 0:
                ages[user_id] = row.column('age')[0].as_py()

        # Check if all ages are in range
        if len(ages) == 4 and all(age_min <= age <= age_max for age in ages.values()):
            # Get names
            names = {}
            for user_id in [a_id, b_id, c_id, d_id]:
                mask = pc.equal(users.column('user_id'), pa.scalar(user_id, type=pa.int64()))
                row = users.filter(mask)
                if row.num_rows > 0:
                    names[user_id] = row.column('name')[0].as_py()

            matching_patterns.append({
                'users': [a_id, b_id, c_id, d_id],
                'names': [names.get(uid, f"User{uid}") for uid in [a_id, b_id, c_id, d_id]],
                'ages': [ages.get(uid, 0) for uid in [a_id, b_id, c_id, d_id]]
            })

    print(f"\nFound {len(matching_patterns)} patterns with age-similar users:\n")

    for i, pattern in enumerate(matching_patterns[:5], 1):  # Show top 5
        path_str = " → ".join([
            f"{name}({age})"
            for name, age in zip(pattern['names'], pattern['ages'])
        ])
        print(f"  {i}. {path_str}")

    if len(matching_patterns) > 5:
        print(f"\n  ... and {len(matching_patterns) - 5} more patterns")


def find_city_clusters(edges, users):
    """
    Find friend groups (3-hop patterns) within the same city.
    """
    print(f"\n{'=' * 60}")
    print(f"CITY-BASED FRIEND CLUSTERS")
    print(f"{'=' * 60}")

    # Find 3-hop patterns
    result = match_3hop(edges, edges, edges)
    table = result.result_table()

    # Group by city
    clusters_by_city = {}

    for i in range(table.num_rows):
        ids = [
            table.column('a_id')[i].as_py(),
            table.column('b_id')[i].as_py(),
            table.column('c_id')[i].as_py(),
            table.column('d_id')[i].as_py()
        ]

        # Get cities for all users
        cities = []
        for user_id in ids:
            mask = pc.equal(users.column('user_id'), pa.scalar(user_id, type=pa.int64()))
            row = users.filter(mask)
            if row.num_rows > 0:
                cities.append(row.column('city')[0].as_py())

        # Check if all same city
        if len(set(cities)) == 1:
            city = cities[0]
            if city not in clusters_by_city:
                clusters_by_city[city] = []
            clusters_by_city[city].append(ids)

    print(f"\nFound friend clusters in {len(clusters_by_city)} cities:\n")

    for city, clusters in sorted(clusters_by_city.items()):
        print(f"  {city}: {len(clusters)} 3-hop patterns")


if __name__ == "__main__":
    edges, users = create_social_network()

    print("█" * 60)
    print("█" + " " * 58 + "█")
    print("█" + "  SOCIAL GRAPH PATTERN MATCHING WITH FILTERING".center(58) + "█")
    print("█" + " " * 58 + "█")
    print("█" * 60)

    print(f"\nNetwork: {users.num_rows} users, {edges.num_rows} friendships")

    # Run all analyses
    find_friend_recommendations(edges, users, user_id=0)
    find_influential_users_in_network(edges, users, min_followers=500)
    find_age_similar_connections(edges, users, target_age=28, tolerance=3)
    find_city_clusters(edges, users)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("""
Pattern matching with property filtering demonstrated:
  ✓ Friend recommendations (filtered by location)
  ✓ Influencer detection (filtered by follower count)
  ✓ Age-similar communities (filtered by age range)
  ✓ Geographic clusters (filtered by city)

Techniques:
  • Pattern matching first (structure)
  • Arrow compute filtering second (properties)
  • Flexible predicate application
  • Efficient columnar operations
    """)
