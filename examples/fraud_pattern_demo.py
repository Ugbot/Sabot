"""
Fraud Detection Pattern Matching Demo

Demonstrates using graph pattern matching to detect suspicious transaction patterns:
1. Money laundering chains (A->B->C transfers)
2. Circular money flows (A->B->C->A cycles)
3. Hub accounts (accounts with many incoming/outgoing transfers)
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.query import match_2hop, match_3hop


def create_transaction_graph():
    """
    Create a transaction network with suspicious patterns.

    Transaction patterns:
    - Normal transactions: 0->1, 1->2, 3->4
    - Suspicious 2-hop chain: 5->6->7 (layering)
    - Suspicious 3-hop chain: 10->11->12->13 (complex layering)
    - Circular flow: 20->21->22->20 (integration)
    - Hub account (25): 23->25, 24->25, 26->25, 25->27, 25->28 (smurfing)
    """
    transactions = pa.table({
        'source': pa.array([
            # Normal transactions
            0, 1, 3,
            # Suspicious 2-hop chain (layering)
            5, 6,
            # Suspicious 3-hop chain (complex layering)
            10, 11, 12,
            # Circular flow (integration)
            20, 21, 22,
            # Hub account (smurfing/structuring)
            23, 24, 26, 25, 25,
        ], type=pa.int64()),
        'target': pa.array([
            # Normal
            1, 2, 4,
            # 2-hop chain
            6, 7,
            # 3-hop chain
            11, 12, 13,
            # Circular
            21, 22, 20,
            # Hub
            25, 25, 25, 27, 28,
        ], type=pa.int64())
    })

    # Account names for readability
    account_names = {
        0: "Alice", 1: "Bob", 2: "Carol", 3: "Dave", 4: "Eve",
        5: "Suspicious_A", 6: "Mule_B", 7: "Destination_C",
        10: "Origin_X", 11: "Layer1_Y", 12: "Layer2_Z", 13: "Final_W",
        20: "Circular_P", 21: "Circular_Q", 22: "Circular_R",
        23: "Depositor1", 24: "Depositor2", 25: "HUB_ACCOUNT",
        26: "Depositor3", 27: "Withdraw1", 28: "Withdraw2"
    }

    return transactions, account_names


def detect_layering_2hop(transactions, account_names):
    """
    Detect 2-hop layering patterns.

    Layering is a money laundering technique where funds are moved through
    multiple accounts to obscure the trail.
    """
    print("\n" + "=" * 60)
    print("1. LAYERING DETECTION (2-hop patterns)")
    print("=" * 60)

    result = match_2hop(
        transactions, transactions,
        source_name="origin",
        intermediate_name="mule",
        target_name="destination"
    )

    print(f"\nFound {result.num_matches()} potential layering patterns (A->B->C)")

    table = result.result_table()
    origins = table.column('origin_id').to_pylist()
    mules = table.column('mule_id').to_pylist()
    destinations = table.column('destination_id').to_pylist()

    print("\nSuspicious 2-hop transfer chains:")
    for i, (origin, mule, dest) in enumerate(zip(origins, mules, destinations), 1):
        origin_name = account_names.get(origin, f"Account_{origin}")
        mule_name = account_names.get(mule, f"Account_{mule}")
        dest_name = account_names.get(dest, f"Account_{dest}")

        # Flag suspicious patterns (accounts with "Suspicious" or "Mule" in name)
        is_suspicious = "Suspicious" in origin_name or "Mule" in mule_name
        flag = "🚨 SUSPICIOUS" if is_suspicious else "✓ Normal"

        print(f"  {i}. {origin_name} → {mule_name} → {dest_name}  {flag}")


def detect_complex_layering_3hop(transactions, account_names):
    """
    Detect 3-hop layering patterns (more complex money laundering).

    Longer chains indicate more sophisticated attempts to hide the money trail.
    """
    print("\n" + "=" * 60)
    print("2. COMPLEX LAYERING DETECTION (3-hop patterns)")
    print("=" * 60)

    result = match_3hop(transactions, transactions, transactions)

    print(f"\nFound {result.num_matches()} complex layering patterns (A->B->C->D)")

    table = result.result_table()
    a_ids = table.column('a_id').to_pylist()
    b_ids = table.column('b_id').to_pylist()
    c_ids = table.column('c_id').to_pylist()
    d_ids = table.column('d_id').to_pylist()

    print("\nSuspicious 3-hop transfer chains:")
    for i, (a, b, c, d) in enumerate(zip(a_ids, b_ids, c_ids, d_ids), 1):
        a_name = account_names.get(a, f"Account_{a}")
        b_name = account_names.get(b, f"Account_{b}")
        c_name = account_names.get(c, f"Account_{c}")
        d_name = account_names.get(d, f"Account_{d}")

        # Flag chains with "Layer" accounts
        is_suspicious = "Layer" in b_name or "Layer" in c_name or "Origin" in a_name
        flag = "🚨 HIGH RISK" if is_suspicious else "✓ Normal"

        print(f"  {i}. {a_name} → {b_name} → {c_name} → {d_name}  {flag}")


def detect_circular_flows(transactions, account_names):
    """
    Detect circular money flows (A->B->C->A).

    Circular flows can indicate "round-tripping" to create false volume
    or to integrate illicit funds back into the legitimate economy.
    """
    print("\n" + "=" * 60)
    print("3. CIRCULAR FLOW DETECTION")
    print("=" * 60)

    result = match_3hop(transactions, transactions, transactions)

    # Find cycles: paths where A == D
    table = result.result_table()
    a_ids = table.column('a_id').to_pylist()
    b_ids = table.column('b_id').to_pylist()
    c_ids = table.column('c_id').to_pylist()
    d_ids = table.column('d_id').to_pylist()

    cycles = []
    for a, b, c, d in zip(a_ids, b_ids, c_ids, d_ids):
        if a == d:  # Found a cycle
            cycles.append((a, b, c))

    print(f"\nFound {len(cycles)} circular money flows:")

    for i, (a, b, c) in enumerate(cycles, 1):
        a_name = account_names.get(a, f"Account_{a}")
        b_name = account_names.get(b, f"Account_{b}")
        c_name = account_names.get(c, f"Account_{c}")

        print(f"  {i}. 🚨 CIRCULAR: {a_name} → {b_name} → {c_name} → {a_name}")
        print(f"     Risk: High (round-tripping or integration)")


def detect_hub_accounts(transactions, account_names):
    """
    Detect hub accounts with many connections.

    Hub accounts with many incoming transfers from different sources
    and/or many outgoing transfers can indicate "smurfing" or "structuring".
    """
    print("\n" + "=" * 60)
    print("4. HUB ACCOUNT DETECTION (Smurfing/Structuring)")
    print("=" * 60)

    # Count incoming and outgoing edges for each account
    sources = transactions.column('source').to_pylist()
    targets = transactions.column('target').to_pylist()

    incoming = {}
    outgoing = {}

    for src, tgt in zip(sources, targets):
        outgoing[src] = outgoing.get(src, 0) + 1
        incoming[tgt] = incoming.get(tgt, 0) + 1

    # Find hub accounts (3+ incoming or 3+ outgoing)
    hubs = set()
    for acct in set(sources + targets):
        in_count = incoming.get(acct, 0)
        out_count = outgoing.get(acct, 0)
        if in_count >= 3 or out_count >= 3:
            hubs.add((acct, in_count, out_count))

    print(f"\nFound {len(hubs)} potential hub accounts:")

    for acct, in_count, out_count in sorted(hubs, key=lambda x: x[1] + x[2], reverse=True):
        acct_name = account_names.get(acct, f"Account_{acct}")

        # Flag accounts with suspicious names or high activity
        is_suspicious = "HUB" in acct_name or (in_count >= 3 and out_count >= 2)
        flag = "🚨 SUSPICIOUS" if is_suspicious else "⚠️  Monitor"

        print(f"  • {acct_name}  {flag}")
        print(f"    Incoming: {in_count} | Outgoing: {out_count}")

        if in_count >= 3:
            print(f"    Pattern: Multiple deposits (possible smurfing)")
        if out_count >= 3:
            print(f"    Pattern: Multiple withdrawals (possible layering)")


def generate_fraud_report(transactions, account_names):
    """Generate complete fraud detection report"""
    print("\n" + "█" * 60)
    print("█" + " " * 58 + "█")
    print("█" + "  FRAUD DETECTION REPORT - Pattern Matching Analysis".center(58) + "█")
    print("█" + " " * 58 + "█")
    print("█" * 60)

    print(f"\nDataset: {transactions.num_rows} transactions across {len(account_names)} accounts")

    # Run all detectors
    detect_layering_2hop(transactions, account_names)
    detect_complex_layering_3hop(transactions, account_names)
    detect_circular_flows(transactions, account_names)
    detect_hub_accounts(transactions, account_names)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("""
Pattern matching successfully identified:
  ✓ Simple layering (2-hop chains)
  ✓ Complex layering (3-hop chains)
  ✓ Circular flows (integration)
  ✓ Hub accounts (smurfing/structuring)

Recommended Actions:
  1. Flag suspicious accounts for manual review
  2. Monitor circular flows for AML compliance
  3. Investigate hub accounts with transaction history
  4. Apply graph analytics for network analysis
    """)


if __name__ == "__main__":
    transactions, account_names = create_transaction_graph()
    generate_fraud_report(transactions, account_names)
