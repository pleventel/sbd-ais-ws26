# Exercise 6 — *Card Sentinel*: real-time fraud detection with CDC

## Setup

> PostgreSQL, Kafka and the Kafka UI are the same images as in Exercise 5.
> New is **Kafka Connect with Debezium** (~1.4 GB). Download it beforehand:
> 
> ```bash
> docker compose pull
> ```
> 
> Prepare a Python virtual environment in this folder (Windows: `python` or
> `py` instead of `python3`, `venv\Scripts\activate` instead of
> `source venv/bin/activate`):
> 
> ```bash
> python3 -m venv venv
> source venv/bin/activate
> pip install --upgrade pip
> pip install -r requirements.txt
> ```
> 
> Stop the containers of Exercise 5 (`docker compose down -v` in its folder),
> then start this exercise's environment:
> 
> ```bash
> docker compose up -d
> ```
> 
> `docker ps` should show `postgres`, `kafka`, `connect` and `kafka-ui`, and
> <http://localhost:8083/connector-plugins> should list the Debezium connectors
> (it can take ~30 s after start-up).

<center>

🥳 **Everything works!** Proceeding with the tasks.
</center>

## *Card Sentinel* — fraud detection on a system of record

> **Scenario.** A card processor commits every card payment into PostgreSQL,
> its *system of record* (consistency, durability, transactional integrity).
> The payment path belongs to another team and is certified: you may **not**
> change it. The data science team has delivered two fraud models, and case
> management wants their alerts within seconds, so that a stolen card is
> blocked before the thief's next payment.
> 
> The lab runs 1,000 payments per second for 100,000 cardholders.
> 
> ![The pipeline: card_processor.py (the payment path, do not change) commits 1,000 payments/s into the table transactions of PostgreSQL; every commit goes into the write-ahead log (WAL), and a replication slot remembers how far Debezium has read. Kafka Connect runs the Debezium connector (your file fraud-connector.json) and writes into the topic fraud.public.transactions, whose number of partitions and key you set in Part 2. Two consumer groups read the topic: anomaly-agent with ano-1, and velocity-agent with vel-1, vel-2 and vel-3, all started by agents.py. Both write their alerts to the topic fraud-alerts, which scoreboard.py checks against the ground truth (R2–R4).](images/card-sentinel-pipeline.png)
> 
> **Requirements**
> 
> | | Requirement |
> |---|---|
> | R1 | **System of record untouched:** the payment path (`card_processor.py`) stays unchanged, never waits for a fraud check, and gets no extra query load. |
> | R2 | **Every payment to every model:** each model gets every committed payment, none is missed. Adding another consumer (e.g. a training-data archive for the data science team) must not change what the others get. |
> | R3 | **Correct at any scale:** each model catches ≥ 90 % of its frauds with few false alerts, *however many instances of it run*. |
> | R4 | **Fresh:** an alert at most 5 s (p95) after the payment was committed, at full rate. |

### Part 1 — The decision (in the session)

> There are four common ways to get every committed payment out of a
> database and to the models (lecture: *Getting data out of a system of
> record: four options* and *Why two writes are not one*):
> 
> **Your task (for the write-up):** one paragraph, 5–8 sentences. Which
> option do you choose for this scenario, and why? What does it cost, and how
> would you notice in production before it hurts? Which option would you
> choose if your team *owned* the payment path?

I choose **log-based CDC with Debezium** (option 4).
- *Polling* would miss payments that commit out of `id` order and would add query load to the system of record for every agent, which breaks R1 and R2.
- *Dual write* and the *Outbox* both change the certified payment path, and dual write also loses an event if the processor crashes between the commit and the send to Kafka.

Debezium reads the write-ahead log, so the payment path stays untouched, every committed payment reaches Kafka in commit order, and each model reads it through its own consumer group, so a further consumer (e.g. an archive) changes nothing for the others. The cost is the replication slot: `PostgreSQL` keeps all write-ahead log (WAL) the connector has not read yet, so a connector that is down for a weekend could fill the disk and stop the database and the payment path with it. I would notice this early by monitoring the connector status, the WAL retained by the slot (`pg_replication_slots`, `confirmed_flush_lsn` against `pg_current_wal_lsn()`) and the free disk space, with alerts well before the disk is full.

If my team owned the payment path, I would choose the **outbox**: it needs no access to the WAL, writes the event in the same transaction as the payment, and makes the published event an explicit contract instead of a copy of the table.

### Part 2 — The pipeline in three runs (in the session)

> **How many agents.** The velocity model costs 2 ms per payment, so one
> instance handles at most 500 payments/s, in practice 400–500 (reading the
> JSON events costs time too). For 1,000 payments/s we run **3 velocity
> instances**. The anomaly model (0.3 ms) manages with **one**. `agents.py`
> starts exactly these. (Lecture: *Keys and partitions are capacity
> decisions*.)
> 
> Use three terminals in `fraud/`, virtual environment active:
> 
> 1. **Stop everything from the previous run:** `Ctrl+C` in the terminal of
>    the agents and in the one of the card processor. (Before Run 1 there is
>    nothing to stop.)
> 2. **Terminal 1: register the connector** in `fraud-connector.json`. If a
>    previous one exists, this removes it, with its replication slot and its
>    topic, so the new topic is created with your new settings. It should
>    print `RUNNING`:
> 
>    ```bash
>    cd fraud
>    python3 connector.py fraud-connector.json
>    ```
> 
> 3. **Terminal 1: start the payment path,** and keep it running. Every start
>    is a new run; the scoreboard only counts the current one.
> 
>    ```bash
>    python3 card_processor.py
>    ```
> 
> 4. **Terminal 2: start the agents** (1 anomaly, 3 velocity) on the new
>    topic.
> 
>    ```bash
>    python3 agents.py
>    ```
> 
> 5. **Wait about 2 minutes** (the first fraud comes after 30 s, then one
>    every 8 s), **then terminal 3: the scoreboard.**
> 
>    ```bash
>    python3 scoreboard.py
>    ```
> 
> For every run, note in your write-up: your **prediction**, the
> **velocity lines** of the scoreboard, and **why**, in your own words.

#### Run 1 — Debezium's defaults

**Prediction:** all three velocity instances share the work and R4 passes, because three instances were started for 1,000 payments/s.

```text
Run 1794997635: started 11:27:15, 185,325 transactions in 185 s (1,001/s), 20 frauds so far (still running)

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   1        1     3,602
  velocity-agent           fraud.public.transactions                   1        3    61,756

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1         174495 anomaly         186,627       84,508             0      0.9       2s ago
  vel-1         174496 velocity         50,127       39,477             0     59.2       3s ago

Alerts on 'fraud-alerts' (committed, this run)

  velocity model (card testing)
    R3  frauds caught          3/10  (30%)   FAIL  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  41.6 s / 59.4 s   FAIL  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

  anomaly model (account takeover)
    R3  frauds caught          10/10  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  10.7 s / 54.0 s   FAIL  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)
```

**Why:** the topic was created with only **1 partition** (`assigned partitions [0]` for `vel-1`, `[]` for `vel-2` and `vel-3`). Inside a consumer group one partition is read by exactly one instance, so partitions cap parallelism and the other two instances stay idle. `vel-1` handles only about 400–550 of the 1,000 payments/s, its lag grows every second and R4 fails.

#### Run 2 — six partitions

> Add these two lines inside `"config"` of `fraud-connector.json` (mind the
> commas; replication factor 1, because we have a single broker):
> 
> ```json
>     "topic.creation.default.partitions": "6",
>     "topic.creation.default.replication.factor": "1",
> ```

**Prediction:** all three instances now work and R4 passes, but I expected R3 to pass as well, since every instance reads its own partitions.

```text
Run 1794998171: started 11:36:11, 135,285 transactions in 135 s (1,001/s), 14 frauds so far (still running)

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   6        1     3,102
  velocity-agent           fraud.public.transactions                   6        3     3,102

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1         206349 anomaly         139,189       75,051             0      0.6       0s ago
  vel-1         206350 velocity         43,767       35,442             1      0.6       2s ago
  vel-2         206351 velocity         43,623       35,273             0      0.6       1s ago
  vel-3         206352 velocity         43,841       35,444             0      0.6       1s ago

Alerts on 'fraud-alerts' (committed, this run)

  velocity model (card testing)
    R3  frauds caught          0/7  (0%)   FAIL  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  no alerts; worst lag of the velocity agents 0.7 s   PASS  (<= 5 s)
        duplicate alerts       0   (info: alerts case management got twice)

  anomaly model (account takeover)
    R3  frauds caught          7/7  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.5 s / 0.7 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)
```

**Why:** each instance now has two partitions (`[0, 1]`, `[2, 3]`, `[4, 5]`) and keeps up, so R4 passes. But the key of an event is still the primary key `id`, so `hash(key) mod 6` spreads the 12 test payments of one stolen card over all partitions and all three instances. Each instance sees only about 4 of them in 60 s, below the threshold of 8, so almost no fraud is caught and R3 fails. The anomaly model still passes because its single instance reads all six partitions.

### Run 3 — the cardholder as the key

> Add this line inside `"config"`:
> 
> ```json
>     "message.key.columns": "public.transactions:user_id",
> ```

**Prediction:** the key in the Kafka UI is `{"user_id": ...}`, all payments of a cardholder land in one partition, and R3 and R4 both pass.

```text
Run 1794998530: started 11:42:10, 190,329 transactions in 190 s (1,001/s), 21 frauds so far (still running)

Consumer groups on the CDC topic(s)   [R2]
  group                    topic                              partitions  members       lag
  anomaly-agent            fraud.public.transactions                   6        1     4,428
  velocity-agent           fraud.public.transactions                   6        3     4,494

Agent instances (telemetry of this run)   [R3, R4]
  instance         pid model      transactions  cardholders  out-of-order  lag (s)      seen
  ano-1         230670 anomaly         193,934       85,701             0      0.8       0s ago
  vel-1         230671 velocity         63,230       28,693             0      0.9       1s ago
  vel-2         230672 velocity         62,347       28,933             0      0.9       3s ago
  vel-3         230673 velocity         61,612       28,033             0      0.9       1s ago

Alerts on 'fraud-alerts' (committed, this run)

  velocity model (card testing)
    R3  frauds caught          10/10  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.9 s / 1.0 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)

  anomaly model (account takeover)
    R3  frauds caught          10/10  (100%)   PASS  (>= 90 %)
    R3  false alerts           1   PASS  (at most 10 % of the flagged transactions)
    R4  alert latency p50/p95  0.8 s / 1.0 s   PASS  (p95 <= 5 s after the commit)
        duplicate alerts       0   (info: alerts case management got twice)
```

**Why:** `message.key.columns` makes the cardholder the key, so all payments of one cardholder go to the same partition, are read by the same instance and arrive in commit order. That instance counts all 12 test payments and alerts at the 8th, so R3 passes, and the load is split over three instances, so R4 passes. The key must be the entity whose state the model keeps. An occasional false alert is expected: an honest cardholder who pays 8 times within a minute looks like a thief to a threshold rule.

---

### Closing

Debezium chose **one partition** and the **primary key as the event key**. Nothing would have crashed and no error would have appeared. With one partition, only one velocity instance works, so the lag of the fraud alerts grows and a stolen card is blocked minutes too late. After adding partitions but keeping the primary key, the pipeline looks healthy (instances busy, lag low), yet the velocity model silently catches almost no card-testing, because no instance sees a whole attack. The team could have noticed this with monitoring and tests: consumer lag per group, the number of partitions assigned per instance (idle members are a warning sign), the recall of alerts against confirmed chargebacks, and an end-to-end test that injects known frauds, as the scoreboard does. Debezium's defaults should also be reviewed explicitly (partitions, key, encoding) before a connector goes to production.