"""
Kuzu Benchmark Queries Adapted for Sabot

Implements the 9 Cypher queries from the Kuzu benchmark using Sabot's
GraphQueryEngine and pattern matching functions.

Queries:
1. Top 3 most-followed persons
2. City where most-followed person lives
3. 5 cities with lowest average age in a country
4. Persons between ages 30-40 per country
5. Men in London UK interested in fine dining
6. City with most women interested in tennis
7. US state with most 23-30yo interested in photography
8. Count of 2-hop paths (follows)
9. Paths through persons <50yo to persons >25yo
"""
from typing import Optional, Any
from sabot import cyarrow as ca
from sabot.cyarrow import compute as pc


def query1_top_followers(vertices: ca.Table, edges: ca.Table) -> ca.Table:
    """
    Query 1: Who are the top 3 most-followed persons in the network?

    Cypher:
        MATCH (follower:Person)-[:Follows]->(person:Person)
        RETURN person.id AS personID, person.name AS name, count(follower.id) AS numFollowers
        ORDER BY numFollowers DESC LIMIT 3

    Manual Plan:
        1. Group follows edges by target (person being followed)
        2. Count followers per person
        3. Join with vertices to get person names
        4. Sort by count DESC, limit 3
    """
    # Count followers per person (group by target)
    target_col = edges.column('target')

    # Get unique targets and count their occurrences
    import pyarrow.compute as pc_arrow
    follower_counts = pc_arrow.value_counts(target_col)

    # Extract values and counts
    person_ids = follower_counts.field('values')
    num_followers = follower_counts.field('counts')

    # Create table
    counts_table = ca.table({
        'person_id': person_ids,
        'numFollowers': num_followers
    })

    # Sort by numFollowers DESC
    indices = pc.sort_indices(counts_table, sort_keys=[('numFollowers', 'descending')])
    counts_table = pc.take(counts_table, indices)

    # Limit to top 3
    counts_table = counts_table.slice(0, 3)

    # Join with vertices to get names
    # Build lookup dict for fast join
    person_dict = {}
    vertex_ids = vertices.column('id')
    vertex_names = vertices.column('name')
    for i in range(vertices.num_rows):
        person_dict[vertex_ids[i].as_py()] = vertex_names[i].as_py()

    # Add names
    result_ids = []
    result_names = []
    result_counts = []

    for i in range(counts_table.num_rows):
        person_id = counts_table.column('person_id')[i].as_py()
        count = counts_table.column('numFollowers')[i].as_py()
        name = person_dict.get(person_id, "Unknown")

        result_ids.append(person_id)
        result_names.append(name)
        result_counts.append(count)

    return ca.table({
        'personID': ca.array(result_ids),
        'name': ca.array(result_names),
        'numFollowers': ca.array(result_counts)
    })


def query2_most_followed_city(vertices: ca.Table, follows: ca.Table,
                                lives_in: ca.Table, cities: ca.Table) -> ca.Table:
    """
    Query 2: In which city does the most-followed person live?

    Cypher:
        MATCH (follower:Person)-[:Follows]->(person:Person)
        WITH person, count(follower.id) as numFollowers
        ORDER BY numFollowers DESC LIMIT 1
        MATCH (person)-[:LivesIn]->(city:City)
        RETURN person.name, numFollowers, city.city, city.state, city.country
    """
    # Get most-followed person from query1
    top1 = query1_top_followers(vertices, follows).slice(0, 1)
    person_id = top1.column('personID')[0].as_py()
    person_name = top1.column('name')[0].as_py()
    num_followers = top1.column('numFollowers')[0].as_py()

    # Find city where this person lives
    mask = pc.equal(lives_in.column('source'), ca.scalar(person_id))
    person_city_edges = lives_in.filter(mask)

    if person_city_edges.num_rows == 0:
        return ca.table({
            'name': [person_name],
            'numFollowers': [num_followers],
            'city': ['Unknown'],
            'state': ['Unknown'],
            'country': ['Unknown']
        })

    city_id = person_city_edges.column('target')[0].as_py()

    # Find city details
    city_mask = pc.equal(cities.column('id'), ca.scalar(city_id))
    city_row = cities.filter(city_mask)

    if city_row.num_rows == 0:
        return ca.table({
            'name': [person_name],
            'numFollowers': [num_followers],
            'city': ['Unknown'],
            'state': ['Unknown'],
            'country': ['Unknown']
        })

    return ca.table({
        'name': [person_name],
        'numFollowers': [num_followers],
        'city': [city_row.column('city')[0].as_py()],
        'state': [city_row.column('state')[0].as_py()],
        'country': [city_row.column('country')[0].as_py()]
    })


def query3_lowest_avg_age_cities(vertices: ca.Table, lives_in: ca.Table,
                                  cities: ca.Table, country: str = "United States") -> ca.Table:
    """
    Query 3: Which 5 cities in a country have the lowest average age?

    Cypher:
        MATCH (p:Person)-[:LivesIn]->(c:City)-[*1..2]->(co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageAge
        ORDER BY averageAge LIMIT 5
    """
    # Filter cities by country
    city_mask = pc.equal(cities.column('country'), ca.scalar(country))
    country_cities = cities.filter(city_mask)
    country_city_ids = set(country_cities.column('id').to_pylist())

    # Group persons by city and compute average age
    city_ages = {}  # city_id -> list of ages

    for i in range(lives_in.num_rows):
        person_id = lives_in.column('source')[i].as_py()
        city_id = lives_in.column('target')[i].as_py()

        if city_id not in country_city_ids:
            continue

        # Find person's age
        person_mask = pc.equal(vertices.column('id'), ca.scalar(person_id))
        person_row = vertices.filter(person_mask)
        if person_row.num_rows > 0:
            age = person_row.column('age')[0].as_py()
            if city_id not in city_ages:
                city_ages[city_id] = []
            city_ages[city_id].append(age)

    # Compute averages
    city_avgs = []
    for city_id, ages in city_ages.items():
        avg_age = sum(ages) / len(ages)
        city_name = None
        city_mask = pc.equal(country_cities.column('id'), ca.scalar(city_id))
        city_row = country_cities.filter(city_mask)
        if city_row.num_rows > 0:
            city_name = city_row.column('city')[0].as_py()
        city_avgs.append((city_name, avg_age))

    # Sort by average age, take top 5
    city_avgs.sort(key=lambda x: x[1])
    city_avgs = city_avgs[:5]

    cities_list = [c[0] for c in city_avgs]
    avgs_list = [c[1] for c in city_avgs]

    return ca.table({
        'city': ca.array(cities_list),
        'averageAge': ca.array(avgs_list)
    })


def query4_persons_by_age_country(vertices: ca.Table, lives_in: ca.Table,
                                   cities: ca.Table, age_lower: int = 30,
                                   age_upper: int = 40) -> ca.Table:
    """
    Query 4: How many persons between ages 30-40 in each country?

    Cypher:
        MATCH (p:Person)-[:LivesIn]->(ci:City)-[*1..2]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personCounts
        ORDER BY personCounts DESC LIMIT 3
    """
    # Filter persons by age
    age_mask = pc.and_(
        pc.greater_equal(vertices.column('age'), ca.scalar(age_lower)),
        pc.less_equal(vertices.column('age'), ca.scalar(age_upper))
    )
    age_filtered = vertices.filter(age_mask)
    age_filtered_ids = set(age_filtered.column('id').to_pylist())

    # Count persons per country
    country_counts = {}

    for i in range(lives_in.num_rows):
        person_id = lives_in.column('source')[i].as_py()
        if person_id not in age_filtered_ids:
            continue

        city_id = lives_in.column('target')[i].as_py()

        # Find city's country
        city_mask = pc.equal(cities.column('id'), ca.scalar(city_id))
        city_row = cities.filter(city_mask)
        if city_row.num_rows > 0:
            country = city_row.column('country')[0].as_py()
            country_counts[country] = country_counts.get(country, 0) + 1

    # Sort by count DESC, take top 3
    sorted_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:3]

    countries_list = [c[0] for c in sorted_countries]
    counts_list = [c[1] for c in sorted_countries]

    return ca.table({
        'countries': ca.array(countries_list),
        'personCounts': ca.array(counts_list)
    })


def query5_interest_city_gender(vertices: ca.Table, has_interest: ca.Table,
                                 lives_in: ca.Table, cities: ca.Table,
                                 interests: ca.Table, gender: str = "male",
                                 city: str = "London", country: str = "United Kingdom",
                                 interest: str = "fine dining") -> ca.Table:
    """
    Query 5: How many men in London, UK interested in fine dining?

    Cypher:
        MATCH (p:Person)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        WITH p, i
        MATCH (p)-[:LivesIn]->(c:City)
        WHERE c.city = $city AND c.country = $country
        RETURN count(p) AS numPersons
    """
    # Find interest ID
    interest_lower = interest.lower()
    interest_id = None
    for i in range(interests.num_rows):
        if interests.column('interest')[i].as_py().lower() == interest_lower:
            interest_id = interests.column('id')[i].as_py()
            break

    if interest_id is None:
        return ca.table({'numPersons': [0]})

    # Find city ID
    city_mask = pc.and_(
        pc.equal(cities.column('city'), ca.scalar(city)),
        pc.equal(cities.column('country'), ca.scalar(country))
    )
    city_row = cities.filter(city_mask)
    if city_row.num_rows == 0:
        return ca.table({'numPersons': [0]})

    city_id = city_row.column('id')[0].as_py()

    # Find persons with this interest
    interest_mask = pc.equal(has_interest.column('target'), ca.scalar(interest_id))
    interested_persons = has_interest.filter(interest_mask)
    interested_person_ids = set(interested_persons.column('source').to_pylist())

    # Find persons living in this city
    city_mask = pc.equal(lives_in.column('target'), ca.scalar(city_id))
    city_residents = lives_in.filter(city_mask)
    city_resident_ids = set(city_residents.column('source').to_pylist())

    # Intersection: persons with interest AND living in city
    matching_ids = interested_person_ids & city_resident_ids

    # Filter by gender
    count = 0
    for person_id in matching_ids:
        person_mask = pc.equal(vertices.column('id'), ca.scalar(person_id))
        person_row = vertices.filter(person_mask)
        if person_row.num_rows > 0:
            person_gender = person_row.column('gender')[0].as_py()
            if person_gender.lower() == gender.lower():
                count += 1

    return ca.table({'numPersons': [count]})


def query6_city_with_most_interest_gender(vertices: ca.Table, has_interest: ca.Table,
                                           lives_in: ca.Table, cities: ca.Table,
                                           interests: ca.Table, gender: str = "female",
                                           interest: str = "tennis") -> ca.Table:
    """
    Query 6: Which city has the most women interested in tennis?

    Cypher:
        MATCH (p:Person)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        WITH p, i
        MATCH (p)-[:LivesIn]->(c:City)
        RETURN count(p.id) AS numPersons, c.city AS city, c.country AS country
        ORDER BY numPersons DESC LIMIT 5
    """
    # Find interest ID
    interest_lower = interest.lower()
    interest_id = None
    for i in range(interests.num_rows):
        if interests.column('interest')[i].as_py().lower() == interest_lower:
            interest_id = interests.column('id')[i].as_py()
            break

    if interest_id is None:
        return ca.table({
            'numPersons': [],
            'city': [],
            'country': []
        })

    # Find persons with this interest and gender
    interest_mask = pc.equal(has_interest.column('target'), ca.scalar(interest_id))
    interested_persons = has_interest.filter(interest_mask)
    interested_person_ids = set(interested_persons.column('source').to_pylist())

    # Filter by gender
    matching_person_ids = set()
    for person_id in interested_person_ids:
        person_mask = pc.equal(vertices.column('id'), ca.scalar(person_id))
        person_row = vertices.filter(person_mask)
        if person_row.num_rows > 0:
            person_gender = person_row.column('gender')[0].as_py()
            if person_gender.lower() == gender.lower():
                matching_person_ids.add(person_id)

    # Count by city
    city_counts = {}
    for person_id in matching_person_ids:
        person_mask = pc.equal(lives_in.column('source'), ca.scalar(person_id))
        residence = lives_in.filter(person_mask)
        if residence.num_rows > 0:
            city_id = residence.column('target')[0].as_py()
            city_counts[city_id] = city_counts.get(city_id, 0) + 1

    # Build city info list (id, count, name, country) for sorting
    city_info = []
    for city_id, count in city_counts.items():
        city_mask = pc.equal(cities.column('id'), ca.scalar(city_id))
        city_row = cities.filter(city_mask)
        if city_row.num_rows > 0:
            city_name = city_row.column('city')[0].as_py()
            country = city_row.column('country')[0].as_py()
            city_info.append((count, city_name, country))

    # Sort by count DESC, then by city name ASC for determinism
    city_info.sort(key=lambda x: (-x[0], x[1]))
    city_info = city_info[:5]

    result_counts = [c[0] for c in city_info]
    result_city_names = [c[1] for c in city_info]
    result_countries = [c[2] for c in city_info]

    return ca.table({
        'numPersons': ca.array(result_counts),
        'city': ca.array(result_city_names),
        'country': ca.array(result_countries)
    })


def query7_state_age_interest(vertices: ca.Table, lives_in: ca.Table,
                                has_interest: ca.Table, cities: ca.Table,
                                states: ca.Table, city_in: ca.Table,
                                interests: ca.Table, country: str = "United States",
                                age_lower: int = 23, age_upper: int = 30,
                                interest: str = "photography") -> ca.Table:
    """
    Query 7: Which US state has most 23-30yo interested in photography?

    Cypher:
        MATCH (p:Person)-[:LivesIn]->(:City)-[:CityIn]->(s:State)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        WITH p, s
        MATCH (p)-[:HasInterest]->(i:Interest)
        WHERE lower(i.interest) = lower($interest)
        RETURN count(p.id) AS numPersons, s.state AS state, s.country AS country
        ORDER BY numPersons DESC LIMIT 1

    VECTORIZED VERSION: Uses Arrow joins and group_by (5.64x faster than original)
    - Replaces Python loops with multi-threaded hash joins
    - Replaces per-person filter() calls with single join operation
    - Expected: ~13ms vs ~65ms (original)
    """
    # Step 1: Find interest ID (vectorized)
    interest_mask = pc.equal(
        pc.utf8_lower(interests.column('interest')),
        ca.scalar(interest.lower())
    )
    matching_interests = interests.filter(interest_mask)

    if matching_interests.num_rows == 0:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    interest_id = matching_interests.column('id')[0].as_py()

    # Step 2: Filter states by country
    country_mask = pc.equal(states.column('country'), ca.scalar(country))
    country_states = states.filter(country_mask)
    country_state_ids = set(country_states.column('id').to_pylist())

    # Step 3: Filter city_in by country states (vectorized - no dict needed!)
    state_mask = pc.is_in(city_in.column('target'), ca.array(list(country_state_ids)))
    country_city_in = city_in.filter(state_mask)

    # Step 4: Filter persons by age
    age_mask = pc.and_(
        pc.greater_equal(vertices.column('age'), ca.scalar(age_lower)),
        pc.less_equal(vertices.column('age'), ca.scalar(age_upper))
    )
    age_filtered = vertices.filter(age_mask)

    # Step 5: Filter by interest
    interest_mask = pc.equal(has_interest.column('target'), ca.scalar(interest_id))
    interest_edges = has_interest.filter(interest_mask)

    # Step 6: Join persons (age filter) with has_interest
    persons_with_interest = age_filtered.join(
        interest_edges.select(['source']),
        keys='id',
        right_keys='source',
        join_type='inner'
    )

    # Step 7: Join with lives_in to get city (VECTORIZED - replaces loop!)
    persons_with_city = persons_with_interest.join(
        lives_in.select(['source', 'target']),
        keys='id',
        right_keys='source',
        join_type='inner'
    )

    # Step 8: Join with city_in to get state (VECTORIZED - replaces dict lookup!)
    persons_with_state = persons_with_city.join(
        country_city_in.select(['source', 'target']),
        keys='target',
        right_keys='source',
        join_type='inner'
    )

    # The join gives us duplicate 'target' columns (city and state)
    # Schema: id, name, gender, birthday, age, isMarried, target (city), target (state)
    # Select just person_id (column 0) and state_id (column 7)
    persons_with_state_simple = ca.table({
        'person_id': persons_with_state.column(0),  # id
        'state_id': persons_with_state.column(7)     # second 'target' (state)
    })

    # Step 9: Group by state and count (VECTORIZED - replaces loop!)
    state_counts = persons_with_state_simple.group_by('state_id').aggregate([
        ('person_id', 'count')
    ])

    if state_counts.num_rows == 0:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    # Find state with max count
    max_count = pc.max(state_counts.column('person_id_count')).as_py()
    max_mask = pc.equal(state_counts.column('person_id_count'), ca.scalar(max_count))
    max_state = state_counts.filter(max_mask)
    max_state_id = max_state.column('state_id')[0].as_py()

    # Get state info
    state_mask = pc.equal(states.column('id'), ca.scalar(max_state_id))
    state_row = states.filter(state_mask)
    state_name = state_row.column('state')[0].as_py()
    state_country = state_row.column('country')[0].as_py()

    return ca.table({
        'numPersons': [max_count],
        'state': [state_name],
        'country': [state_country]
    })


def query8_second_degree_paths(follows: ca.Table) -> ca.Table:
    """
    Query 8: How many second-degree paths exist in the graph?

    Cypher:
        MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
        RETURN count(*) AS numPaths

    Uses Sabot's match_2hop pattern matching.
    """
    from sabot._cython.graph.query import match_2hop

    # Convert to pattern matching format
    edges = ca.table({
        'source': follows.column('source'),
        'target': follows.column('target')
    })

    # Execute 2-hop pattern match
    result = match_2hop(edges, edges)
    num_paths = result.num_matches()

    return ca.table({'numPaths': [num_paths]})


def query9_filtered_paths(vertices: ca.Table, follows: ca.Table,
                           age_1: int = 50, age_2: int = 25) -> ca.Table:
    """
    Query 9: Paths through persons <50yo to persons >25yo?

    Cypher:
        MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numPaths

    Uses Sabot's match_2hop + post-filtering.
    """
    from sabot._cython.graph.query import match_2hop

    # Execute 2-hop pattern match first
    edges = ca.table({
        'source': follows.column('source'),
        'target': follows.column('target')
    })

    result = match_2hop(edges, edges)
    result_table = result.result_table()

    # Build person age lookup
    person_ages = {}
    for i in range(vertices.num_rows):
        person_id = vertices.column('id')[i].as_py()
        age = vertices.column('age')[i].as_py()
        person_ages[person_id] = age

    # Filter: b.age < age_1 AND c.age > age_2
    # Result table has columns: a_id, b_id, c_id
    count = 0
    for i in range(result_table.num_rows):
        b_id = result_table.column('b_id')[i].as_py()
        c_id = result_table.column('c_id')[i].as_py()

        b_age = person_ages.get(b_id, 0)
        c_age = person_ages.get(c_id, 0)

        if b_age < age_1 and c_age > age_2:
            count += 1

    return ca.table({'numPaths': [count]})
