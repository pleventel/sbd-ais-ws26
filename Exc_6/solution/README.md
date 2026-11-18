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
R3  frauds caught          3/10  (30%)   FAIL
R3  false alerts           0   PASS
R4  alert latency p50/p95  41.6 s / 59.4 s   FAIL  (p95 <= 5 s)
```

**Why:** Debezium created the topic with **1 partition**. The scoreboard shows `velocity-agent` with 3 members but 1 partition, and only `vel-1` reports telemetry. Within a consumer group one partition is read by exactly one instance, so partitions cap parallelism and `vel-2` and `vel-3` stay idle. `vel-1` could not keep up with 1,000 payments/s: the group lag was 61,756 payments and `vel-1` was 59.2 s behind, so alerts came about a minute late (R4 fails) and most of the recent frauds had not been processed yet (R3 fails).

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
R3  frauds caught          0/7  (0%)   FAIL
R3  false alerts           0   PASS
R4  no alerts; worst lag of the velocity agents 0.7 s   PASS  (<= 5 s)
```

**Why:** with 6 partitions for 3 instances the load is shared and the lag is below 1 s, so R4 passes. But the key of an event is still the primary key `id`, and `hash(key) mod 6` spreads the 12 test payments of one stolen card over all partitions and all three instances. Each instance sees only about 4 of them within 60 s, below the threshold of 8, so no alert is raised. The cardholder counts show it: the three velocity instances hold 35,442 + 35,273 + 35,444 = 106,159 cardholders, more than the 75,051 that the single anomaly instance saw, so the same cardholders are spread over several instances. The anomaly model still passes because its single instance reads all six partitions.

### Run 3 — the cardholder as the key

> Add this line inside `"config"`:
> 
> ```json
>     "message.key.columns": "public.transactions:user_id",
> ```

**Prediction:** the key in the Kafka UI is `{"user_id": ...}`, all payments of a cardholder land in one partition, and R3 and R4 both pass.

```text
R3  frauds caught          10/10  (100%)   PASS
R3  false alerts           0   PASS
R4  alert latency p50/p95  0.9 s / 1.0 s   PASS  (p95 <= 5 s)
```

**Why:** `message.key.columns` makes the cardholder the key, so all payments of one cardholder go to the same partition, are read by the same instance and arrive in commit order. That instance sees the whole attack and alerts at the 8th payment (R3), and the load is still split over three instances (R4). The cardholder counts confirm it: 28,693 + 28,933 + 28,033 = 85,659 across the velocity instances, almost exactly the 85,701 of the anomaly instance, so every cardholder lives on exactly one instance. There were no false velocity alerts in this run; a few would be acceptable, since an honest cardholder who pays 8 times within a minute looks like a thief to a threshold rule. The key must be the entity whose state the model keeps.

---

### Closing

Debezium chose **one partition** and the **primary key as the event key**, and nothing crashed or printed an error in either case. With one partition the velocity alerts arrive a minute late, so a stolen card would be blocked far too late. After adding partitions but keeping the primary key, the pipeline looked healthy (lag 0.6 s, R4 PASS) while the velocity model caught 0 of 7 frauds. The team could have noticed this with consumer lag per group, the partitions assigned per instance (idle members are a warning sign), the recall of alerts against confirmed chargebacks, and an end-to-end test that injects known frauds, as the scoreboard does. The defaults for partitions, key and encoding should be reviewed explicitly before a connector goes to production.
