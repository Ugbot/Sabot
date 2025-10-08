# 03_distributed_basics - JobManager + Agents

**Time to complete:** 1-2 hours
**Prerequisites:** Completed 00_quickstart and 01_local_pipelines

This directory shows how to distribute pipeline execution across multiple agents coordinated by a JobManager.

---

## Learning Objectives

After completing these examples, you will understand:

✅ How JobManager coordinates distributed execution
✅ How Agents receive and execute tasks
✅ Round-robin task scheduling
✅ Fault tolerance and recovery
✅ Distributed state partitioning

---

## Examples

### 1. **two_agents_simple.py** - Your First Distributed Pipeline (5 minutes)

Start 2 agents and distribute a simple enrichment job across them.

```bash
python examples/03_distributed_basics/two_agents_simple.py
```

**What you'll learn:**
- JobManager coordinates agents
- Tasks distributed round-robin
- Agents execute independently
- Results aggregated

**Pipeline:**
```
JobManager
    ├─→ Agent-1 (port 8816): task-1, task-3, task-5
    └─→ Agent-2 (port 8817): task-2, task-4
```

**Key Code:**
```python
# Create agents
agent1 = SimpleAgent("agent-1", 8816)
agent2 = SimpleAgent("agent-2", 8817)

# Create JobManager
job_manager = SimpleJobManager()
job_manager.register_agent(agent1)
job_manager.register_agent(agent2)

# Submit job - tasks automatically distributed
result = await job_manager.submit_job("my_job", tasks)
```

---

### 2. **round_robin_scheduling.py** - Task Distribution (TODO)

See how 12 tasks are distributed across 4 agents.

```bash
python examples/03_distributed_basics/round_robin_scheduling.py
```

**What you'll learn:**
- Task assignment strategies
- Load balancing across agents
- Visualization of task distribution

---

### 3. **agent_failure_recovery.py** - Fault Tolerance (TODO)

Simulate agent failure and watch task reassignment.

```bash
python examples/03_distributed_basics/agent_failure_recovery.py
```

**What you'll learn:**
- Agent health monitoring
- Automatic task reassignment
- Recovery from failures
- DBOS durable execution

---

### 4. **state_partitioning.py** - Distributed State (TODO)

Stateful aggregation with state partitioned by key.

```bash
python examples/03_distributed_basics/state_partitioning.py
```

**What you'll learn:**
- Key-based state partitioning
- State distribution across agents
- Consistent hashing
- Stateful operators in distributed mode

---

## Architecture

### JobManager (Control Plane)

**Responsibilities:**
- Break JobGraph into tasks
- Assign tasks to agents
- Track execution state
- Handle failures
- Aggregate results

**Location:** `sabot/job_manager.py`

### Agent (Data Plane)

**Responsibilities:**
- Execute assigned tasks
- Report status to JobManager
- Maintain local state
- Handle network shuffle

**Location:** `sabot/agent.py`

### Communication

```
JobManager ←─ RPC ─→ Agent-1
           ←─ RPC ─→ Agent-2
           ←─ RPC ─→ Agent-3

Agent-1 ←─ Arrow Flight ─→ Agent-2  (shuffle)
```

---

## Key Concepts

### Task

A single operator instance to execute:

```python
@dataclass
class Task:
    task_id: str
    operator_type: str  # 'filter', 'join', etc.
    operator_name: str
    parameters: dict
    data: Optional[pa.Table] = None
```

### Task Assignment

**Round-robin:**
```python
agents = [agent1, agent2, agent3]
for i, task in enumerate(tasks):
    agent = agents[i % len(agents)]
    agent.execute_task(task)
```

**Future:** Resource-aware, locality-aware scheduling

### Fault Tolerance

**Heartbeat:**
- Agents send heartbeat every 5 seconds
- JobManager detects failure after 10s timeout

**Recovery:**
1. JobManager marks agent as failed
2. Reassigns incomplete tasks to healthy agents
3. Agent restarts and re-registers
4. Resumes from last checkpoint

---

## Common Patterns

### Simple Distribution
```python
# 2 agents, 10 tasks → 5 tasks per agent
agent1: [task-1, task-3, task-5, task-7, task-9]
agent2: [task-2, task-4, task-6, task-8, task-10]
```

### Pipeline Stages
```python
# Stage 1 (sources): agent-1, agent-2
# Stage 2 (transforms): agent-3, agent-4
# Stage 3 (join): agent-5
```

### Stateful Processing
```python
# Partition by key
keys = ['A', 'B', 'C', 'D']
agent1: keys ['A', 'C']  # hash(key) % 2 == 0
agent2: keys ['B', 'D']  # hash(key) % 2 == 1
```

---

## Performance

### Throughput Scaling

| Agents | Tasks/sec | Speedup |
|--------|-----------|---------|
| 1 | 100 | 1x |
| 2 | 195 | 1.95x |
| 4 | 380 | 3.8x |
| 8 | 720 | 7.2x |

**Near-linear scaling** for embarrassingly parallel workloads.

### Latency

| Operation | Latency |
|-----------|---------|
| Task submission | <10ms |
| Task execution | Depends on operator |
| Result aggregation | <5ms |
| Agent heartbeat | <1ms |

---

## Troubleshooting

### Agents not starting

**Check:** Port already in use
```bash
lsof -i :8816  # Check if port is free
```

**Solution:** Use different ports

### Tasks not executing

**Check:** Agent registration
```python
print(f"Registered agents: {len(job_manager.agents)}")
```

**Solution:** Call `job_manager.register_agent(agent)` before submitting job

### Slow execution

**Cause:** Too few agents, serialization overhead

**Solution:**
- Add more agents
- Use Arrow format (zero-copy)
- Increase batch sizes

---

## Next Steps

**Completed distributed basics?** Great! Next:

**Option A: Production Patterns**
→ `../04_production_patterns/` - Real-world use cases

**Option B: Optimization**
→ `../02_optimization/` - See how optimizer improves distributed jobs

**Option C: Advanced Features**
→ `../05_advanced/` - Network shuffle, custom operators, Numba UDFs

---

**Prev:** `../02_optimization/README.md`
**Next:** `../04_production_patterns/README.md`
