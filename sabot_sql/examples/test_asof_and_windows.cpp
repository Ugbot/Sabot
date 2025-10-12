#include <iostream>
#include <arrow/api.h>
#include <arrow/result.h>

#include "sabot_sql/sql/simple_sabot_sql_bridge.h"
#include "sabot_sql/sql/sabot_operator_translator.h"

int main() {
    using namespace sabot_sql::sql;
    std::cout << "Testing ASOF JOIN and SAMPLE BY binder + translator...\n";

    // Create bridge
    auto bridge_res = SabotSQLBridge::Create();
    if (!bridge_res.ok()) {
        std::cerr << "Bridge create failed: " << bridge_res.status().ToString() << "\n";
        return 1;
    }
    auto bridge = bridge_res.ValueOrDie();

    // ASOF JOIN query (normalized to LEFT JOIN internally)
    std::string asof_sql =
        "SELECT * FROM trades ASOF JOIN quotes ON trades.symbol = quotes.symbol AND trades.ts <= quotes.ts";
    auto lp_asof = bridge->ParseAndOptimize(asof_sql);
    if (!lp_asof.ok()) {
        std::cerr << "Parse ASOF failed: " << lp_asof.status().ToString() << "\n";
        return 1;
    }
    auto plan_asof = lp_asof.ValueOrDie();
    std::cout << "ASOF flags: has_joins=" << plan_asof.has_joins
              << " has_asof_joins=" << plan_asof.has_asof_joins
              << " ts_col=" << plan_asof.join_timestamp_column << "\n";

    // SAMPLE BY query (window)
    std::string sample_sql =
        "SELECT symbol, avg(price) FROM trades SAMPLE BY 1h";
    auto lp_sample = bridge->ParseAndOptimize(sample_sql);
    if (!lp_sample.ok()) {
        std::cerr << "Parse SAMPLE BY failed: " << lp_sample.status().ToString() << "\n";
        return 1;
    }
    auto plan_sample = lp_sample.ValueOrDie();
    std::cout << "WINDOW flags: has_windows=" << plan_sample.has_windows
              << " interval=" << plan_sample.window_interval << "\n";

    // Translate to morsel plans
    auto tr_res = SabotOperatorTranslator::Create();
    if (!tr_res.ok()) {
        std::cerr << "Translator create failed: " << tr_res.status().ToString() << "\n";
        return 1;
    }
    auto translator = tr_res.ValueOrDie();

    auto mp_asof_res = translator->TranslateToMorselOperators(plan_asof);
    if (!mp_asof_res.ok()) {
        std::cerr << "Translate ASOF failed: " << mp_asof_res.status().ToString() << "\n";
        return 1;
    }
    auto mp_sample_res = translator->TranslateToMorselOperators(plan_sample);
    if (!mp_sample_res.ok()) {
        std::cerr << "Translate SAMPLE failed: " << mp_sample_res.status().ToString() << "\n";
        return 1;
    }

    std::cout << "✓ Binder + translator tests completed" << std::endl;
    return 0;
}


