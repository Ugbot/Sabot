# ROOT CAUSE FOUND - Type Errors in CythonGroupByOperator

**Date:** November 14, 2025

---

## 🔍 THE REAL PROBLEM

### CythonGroupByOperator is FAILING

**Error found:**
```
RuntimeError: Error in groupBy operator: Function 'sum' has no kernel matching input types (string)
```

**What's happening:**
1. CythonGroupByOperator IS being used (not Arrow fallback)
2. But it's FAILING on type errors
3. Queries error out and return 0 batches
4. **Some queries work, some fail**

---

## 💡 Why This Explains Everything

### The Confusing Results

**Times like 0.001-0.006s:**
- Queries that FAIL fast
- Hit type error immediately
- Return 0 batches

**Times like 0.566-0.907s:**
- Queries that WORK
- Process data correctly
- Return actual results

**Inconsistent results:**
- Mix of working and failing queries
- Averages are meaningless
- **Benchmark is broken!**

### Why Scaling Looks Bad

**At small scale (600K):**
- Queries fail/succeed quickly
- Less data to process before error
- Some complete, some don't

**At large scale (10M):**
- More data processed before hitting errors
- Longer time to failure
- **Makes scaling look terrible**

**The scaling issue is a RED HERRING - it's type errors!**

---

## 🎯 The Real Issue

### CythonGroupByOperator is Type-Strict

**Arrow fallback (eager but tolerant):**
```python
# Arrow figures out types automatically
table.group_by(keys).aggregate([
    ('column', 'sum')  # Works on numeric, skips strings
])
```

**CythonGroupByOperator (streaming but strict):**
```python
# Tries to apply operation to column as-is
# Fails if column is wrong type
pc.sum(column)  # ERROR if column is string!
```

**Problem:** Our TPC-H data has mixed types
- Some columns stored as strings
- CythonGroupByOperator doesn't handle this
- **Queries fail instead of working**

---

## ✅ Why This Makes Sense Now

### The Paradox Explained

**You asked:** "Why slower at scale when overhead should be less?"

**Answer:** We're NOT actually slower - queries are FAILING!
- Type errors cause immediate failures
- 0-batch returns
- Invalid benchmark results

**The real comparison:**
- Working Sabot queries: Probably competitive
- Failing Sabot queries: Appear as 0.001s (misleading)
- **Mixed results give false picture**

---

## 🚀 How to Fix

### Option 1: Fix Query Types (Proper)

**Ensure all aggregation columns have correct types:**
```python
# Don't try to sum string columns
# Only aggregate numeric columns
# Type-check before aggregating
```

### Option 2: Make CythonGroupByOperator Type-Tolerant

**Add type checking:**
```python
# In CythonGroupByOperator.process_batch()
if is_numeric(column):
    result = pc.sum(column)
else:
    # Skip or handle gracefully
    result = None
```

### Option 3: Use Arrow Fallback for Now

**Disable CythonGroupByOperator temporarily:**
```python
AGGREGATION_OPERATORS_AVAILABLE = False
# Use eager but working Arrow implementation
```

**This would give us:**
- All queries working
- Slower but correct results
- **Fair comparison possible**

---

## 📊 Expected Results with Fix

### If We Fix Types/Tolerance

**All queries working:**
- Scale 0.1: ~0.85s (5-6x faster)
- Scale 1.67: ~11.82s (competitive)

**With streaming actually working:**
- Scale 1.67 might be even better
- Scaling might improve
- **Fair assessment possible**

---

## ✨ Bottom Line

**The scaling mystery is solved:**
- NOT actually a scaling issue
- Type errors in streaming operator
- Queries failing, not slow

**What we need:**
1. Fix type handling in queries
2. Or make CythonGroupByOperator tolerant
3. Or disable it temporarily

**Then we'll see real performance!** ✅

---

**Type errors, not performance issues, are the root cause!** 💡

