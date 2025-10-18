#!/bin/bash
# Run storage technique benchmarks and track results

set -e

BENCHMARK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BENCHMARK_DIR}/../build"
RESULTS_DIR="${BENCHMARK_DIR}/results"

mkdir -p "${RESULTS_DIR}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get timestamp for results
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
STEP=${1:-"baseline"}

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Storage Techniques Benchmark - ${STEP}${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Build benchmark if needed
if [ ! -f "${BUILD_DIR}/storage_techniques_bench" ]; then
    echo -e "${YELLOW}Building benchmarks...${NC}"
    cd "${BUILD_DIR}"
    cmake ..
    make storage_techniques_bench
fi

# Run benchmarks
echo -e "${GREEN}Running benchmarks...${NC}"
RESULTS_FILE="${RESULTS_DIR}/${STEP}_${TIMESTAMP}.json"

"${BUILD_DIR}/storage_techniques_bench" \
    --benchmark_format=json \
    --benchmark_out="${RESULTS_FILE}" \
    --benchmark_repetitions=3 \
    --benchmark_report_aggregates_only=true

echo -e "${GREEN}Results saved to: ${RESULTS_FILE}${NC}"
echo ""

# Parse and display key metrics
echo -e "${BLUE}Key Metrics Summary:${NC}"
python3 << EOF
import json
import sys

try:
    with open('${RESULTS_FILE}', 'r') as f:
        data = json.load(f)

    benchmarks = data.get('benchmarks', [])

    print("\n┌─────────────────────────────────────────────────────────────┐")
    print("│ Benchmark                                │ Throughput       │")
    print("├─────────────────────────────────────────────────────────────┤")

    for bm in benchmarks:
        name = bm['name'].replace('BM_', '').replace('_mean', '')

        # Calculate throughput
        if 'items_per_second' in bm:
            throughput = bm['items_per_second']
            if throughput > 1000000:
                throughput_str = f"{throughput/1000000:.2f} M/sec"
            elif throughput > 1000:
                throughput_str = f"{throughput/1000:.2f} K/sec"
            else:
                throughput_str = f"{throughput:.2f} /sec"
        else:
            throughput_str = "N/A"

        # Truncate name if too long
        if len(name) > 40:
            name = name[:37] + "..."

        print(f"│ {name:<40} │ {throughput_str:>15} │")

    print("└─────────────────────────────────────────────────────────────┘")

    # Memory usage metrics
    memory_benchmarks = [b for b in benchmarks if 'memory_bytes' in b]
    if memory_benchmarks:
        print("\n┌─────────────────────────────────────────────────────────────┐")
        print("│ Memory Usage                             │ Bytes            │")
        print("├─────────────────────────────────────────────────────────────┤")
        for bm in memory_benchmarks:
            name = bm['name'].replace('BM_', '').replace('_mean', '')
            memory_mb = bm['memory_bytes'] / (1024 * 1024)
            print(f"│ {name:<40} │ {memory_mb:>13.2f} MB │")
        print("└─────────────────────────────────────────────────────────────┘")

except Exception as e:
    print(f"Error parsing results: {e}", file=sys.stderr)
    sys.exit(1)
EOF

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"
echo ""

# Compare with baseline if not baseline
if [ "${STEP}" != "baseline" ] && [ -f "${RESULTS_DIR}/baseline_latest.json" ]; then
    echo -e "${BLUE}Comparing with baseline...${NC}"
    python3 << EOF
import json
import sys

try:
    with open('${RESULTS_FILE}', 'r') as f:
        current = json.load(f)

    with open('${RESULTS_DIR}/baseline_latest.json', 'r') as f:
        baseline = json.load(f)

    current_bms = {b['name']: b for b in current.get('benchmarks', [])}
    baseline_bms = {b['name']: b for b in baseline.get('benchmarks', [])}

    print("\n┌──────────────────────────────────────────────────────────────────────────┐")
    print("│ Benchmark                                │ Change vs Baseline          │")
    print("├──────────────────────────────────────────────────────────────────────────┤")

    for name in sorted(current_bms.keys()):
        if name in baseline_bms:
            curr = current_bms[name]
            base = baseline_bms[name]

            if 'items_per_second' in curr and 'items_per_second' in base:
                curr_tput = curr['items_per_second']
                base_tput = base['items_per_second']

                if base_tput > 0:
                    change_pct = ((curr_tput - base_tput) / base_tput) * 100

                    if change_pct > 0:
                        color = "\033[0;32m"  # Green
                        symbol = "↑"
                    elif change_pct < 0:
                        color = "\033[0;31m"  # Red
                        symbol = "↓"
                    else:
                        color = ""
                        symbol = "="

                    short_name = name.replace('BM_', '').replace('_mean', '')
                    if len(short_name) > 40:
                        short_name = short_name[:37] + "..."

                    print(f"│ {short_name:<40} │ {color}{symbol} {abs(change_pct):>6.2f}%\033[0m               │")

    print("└──────────────────────────────────────────────────────────────────────────┘")

except Exception as e:
    print(f"Error comparing results: {e}", file=sys.stderr)
EOF
fi

# Save as latest for this step
cp "${RESULTS_FILE}" "${RESULTS_DIR}/${STEP}_latest.json"

echo ""
echo -e "${BLUE}Results saved as:${NC}"
echo -e "  ${RESULTS_FILE}"
echo -e "  ${RESULTS_DIR}/${STEP}_latest.json"
echo ""
