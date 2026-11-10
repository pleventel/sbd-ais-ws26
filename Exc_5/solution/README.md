# Exercise 5 Messaging Platforms: Fundamentals

> ## Part 1 — Kafka quick start (guided, not graded)
> 
> Four steps, one idea each; Part 2 builds on all four. In every step,
> **predict** first, then run the commands, then check.
> 
> Open a terminal in `clients/` with the virtual environment active. The
> broker runs at `localhost:9092`. The Kafka command-line tools run inside the
> `kafka` container (`docker exec -it kafka …`); the Kafka UI
> (<http://localhost:8080>) shows the same information in the browser.
> 
> ### 1. A topic is a set of logs
> 
> ```bash
> docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
>   --create --topic clicks --partitions 2 --replication-factor 1
> 
> docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
>   --describe --topic clicks
> ```
> 
> The topic `clicks` has two **partitions**: two append-only logs in which
> every new event gets the next **offset** (0, 1, 2, …). Partitions are the
> unit of parallelism. (`--replication-factor 1`: one copy of each
> partition, because we have a single broker.) Kafka keeps events for 7 days
> by default (`retention.ms`), whether anybody read them or not.
> ### 2. Keys decide the partition
> 
> `producer.py` sends 20 click events with the keys `user-1` … `user-4` and
> prints the partition and offset of each event.
> 
> *Predict:* run it twice. Does `user-1` land in the same partition both
> times? Where do events without a key go?
> 
> ```bash
> python3 producer.py
> python3 producer.py
> python3 producer.py --no-key
> ```
> 
> > **Same key → same partition → same order.** The partition is
> > `hash(key) mod number of partitions`, on every run. Events without a key
> > are spread over both partitions: the order per user is lost.

<details>
<summary style="font-weight: bold;"><i>Predict:</i> run it twice. Does <code>user-1</code> land in the same partition both times? Where do events without a key go?</summary>

#### Expectation:
Running the file twice should result in all users ending up in the same partition.

#### Observation:
The prediction was correct: all users ended up in the same partition.
<details>
<summary><i>Output</i></summary>

~~~text
key=user-1    ->  partition 0  offset 0
key=user-3    ->  partition 1  offset 0
key=user-2    ->  partition 0  offset 1
key=user-1    ->  partition 0  offset 2
key=user-1    ->  partition 0  offset 3
key=user-4    ->  partition 1  offset 1
key=user-1    ->  partition 0  offset 4
key=user-2    ->  partition 0  offset 5
key=user-1    ->  partition 0  offset 6
key=user-2    ->  partition 0  offset 7
key=user-4    ->  partition 1  offset 2
key=user-4    ->  partition 1  offset 3
key=user-3    ->  partition 1  offset 4
key=user-2    ->  partition 0  offset 8
key=user-4    ->  partition 1  offset 5
key=user-3    ->  partition 1  offset 6
key=user-2    ->  partition 0  offset 9
key=user-1    ->  partition 0  offset 10
key=user-4    ->  partition 1  offset 7
key=user-3    ->  partition 1  offset 8
sent 20 events to topic 'clicks'
key=user-1    ->  partition 0  offset 11
key=user-3    ->  partition 1  offset 9
key=user-2    ->  partition 0  offset 12
key=user-1    ->  partition 0  offset 13
key=user-1    ->  partition 0  offset 14
key=user-4    ->  partition 1  offset 10
key=user-1    ->  partition 0  offset 15
key=user-2    ->  partition 0  offset 16
key=user-1    ->  partition 0  offset 17
key=user-2    ->  partition 0  offset 18
key=user-4    ->  partition 1  offset 11
key=user-4    ->  partition 1  offset 12
key=user-3    ->  partition 1  offset 13
key=user-2    ->  partition 0  offset 19
key=user-4    ->  partition 1  offset 14
key=user-3    ->  partition 1  offset 15
key=user-2    ->  partition 0  offset 20
key=user-1    ->  partition 0  offset 21
key=user-4    ->  partition 1  offset 16
key=user-3    ->  partition 1  offset 17
sent 20 events to topic 'clicks'
~~~
</details>

#### Running it without a key:
Without a key, `kafka` distributes messages across the available partitions using its unkeyed partitioning strategy. Both partitions should recieve messages.

`Kafka` uses the message key to determine which partition should receive a message. The producer explicitly configures the `murmur2_random` partitioner, which uses a hash of the key for keyed messages.

Consequently:
- The same key consistently maps to the same partition.
- Different keys may map to the same partition.
- Messages with the same key preserve their relative order within that partition.
- Messages without keys are distributed without guaranteeing that related events reach the same partition.

> #### Key takeaway:
> Same key $\rightarrow$ Same partition $\rightarrow$ Preserved order.
</details>

> ### 3. Consumer groups share the work
> 
> `consumer.py` joins a consumer group and prints the partitions it gets and
> every event it reads. Keep your terminal for the producer and open three
> more in `clients/` (virtual environment active). Start `A`, `B` and `C`
> one after the other; wait each time until the new consumer prints its
> partitions.
> 
> *Predict:* three consumers, two partitions. Who gets what?
> 
> ```bash
> python3 consumer.py --group dashboard --name A
> python3 consumer.py --group dashboard --name B
> python3 consumer.py --group dashboard --name C
> ```
> 
> ![Step 3: producer.py in terminal 4 writes to topic clicks (partitions 0 and 1); group dashboard has consumers A, B and C in terminals 1-3; group billing has consumer X in terminal 4](./../images/part1-step-3.png)
> 
> Send events (`python3 producer.py`): who reads which keys? Then start a
> consumer of a **second** group in the producer's terminal:
> `python3 consumer.py --group billing --name X`. Which events does `X` read?
> 
> > **Within a group, one partition → one consumer.** `C` stays idle: more
> > consumers than partitions do not help. Every join or leave triggers a
> > **rebalance**, and a partition can move to another consumer. Another
> > group (`X`) reads all events on its own, without changing `dashboard`.

<details>
<summary style="font-weight: bold;"><i>Predict:</i> three consumers, two partitions. Who gets what?</summary>

#### Expectation:
There are only two partitions available, so `kafka` can assign work to at most two consumers within the same consumer group at a time.
This means that if *Consumer A* is assigned to **Partition 0** and *Consumer B* is assigned to **Partition 1**, *Consumer C* has no partition to be assigned to.

#### Observation:
What was expected, actually happened.

<details>

##### Terminal 1:
Command: `python3 consumer.py --group dashboard --name A`
~~~text
[A] joined group 'dashboard' on topic 'clicks' (Ctrl+C to stop)
[A] assigned partitions: [0, 1]
[A] revoked partitions:  [0, 1]
[A] assigned partitions: [0]
[A] revoked partitions:  [0]
[A] assigned partitions: [0]
[A] partition 0 offset   28  key=user-1   seq=1
[A] partition 0 offset   29  key=user-2   seq=3
[A] partition 0 offset   30  key=user-1   seq=4
[A] partition 0 offset   31  key=user-1   seq=5
[A] partition 0 offset   32  key=user-1   seq=7
[A] partition 0 offset   33  key=user-2   seq=8
[A] partition 0 offset   34  key=user-1   seq=9
[A] partition 0 offset   35  key=user-2   seq=10
[A] partition 0 offset   36  key=user-2   seq=14
[A] partition 0 offset   37  key=user-2   seq=17
[A] partition 0 offset   38  key=user-1   seq=18
[A] revoked partitions:  [0]
[A] stopped after 11 events
(venv) pleve@pleve-ASUS-Zenbo
~~~

##### Terminal 2:
Command: `python3 consumer.py --group dashboard --name B`
~~~text
[B] joined group 'dashboard' on topic 'clicks' (Ctrl+C to stop)
[B] assigned partitions: [1]
[B] revoked partitions:  [1]
[B] assigned partitions: [1]
[B] partition 1 offset   32  key=user-3   seq=2
[B] partition 1 offset   33  key=user-4   seq=6
[B] partition 1 offset   34  key=user-4   seq=11
[B] partition 1 offset   35  key=user-4   seq=12
[B] partition 1 offset   36  key=user-3   seq=13
[B] partition 1 offset   37  key=user-4   seq=15
[B] partition 1 offset   38  key=user-3   seq=16
[B] partition 1 offset   39  key=user-4   seq=19
[B] partition 1 offset   40  key=user-3   seq=20
[B] revoked partitions:  [1]
[B] assigned partitions: [0]
^C[B] revoked partitions:  [0]
[B] stopped after 9 events

~~~

### Terminal 3&mdash;Before rebalancing:
Command: `python3 consumer.py --group dashboard --name C`
~~~text
[C] joined group 'dashboard' on topic 'clicks' (Ctrl+C to stop)
[C] assigned partitions: none (idle)
~~~

##### Terminal 3&mdash;After rebalancing:
Command: `python3 consumer.py --group dashboard --name C`. State after KeyboardInterrup on `terminal 1` or `terminal 2`.
~~~text
[C] joined group 'dashboard' on topic 'clicks' (Ctrl+C to stop)
[C] assigned partitions: none (idle)
[C] revoked partitions:  []
[C] assigned partitions: [1]
[C] partition 1 offset   37  key=user-4   seq=15
[C] partition 1 offset   38  key=user-3   seq=16
[C] partition 1 offset   39  key=user-4   seq=19
[C] partition 1 offset   40  key=user-3   seq=20
[C] revoked partitions:  [1]
[C] assigned partitions: [0, 1]
[C] partition 1 offset   37  key=user-4   seq=15
[C] partition 1 offset   38  key=user-3   seq=16
[C] partition 1 offset   39  key=user-4   seq=19
[C] partition 1 offset   40  key=user-3   seq=20
[C] revoked partitions:  [0, 1]
[C] stopped after 8 events
~~~

##### Terminal 4:
Command: `python3 producer.py`
~~~text
key=user-1    ->  partition 0  offset 28
key=user-3    ->  partition 1  offset 32
key=user-2    ->  partition 0  offset 29
key=user-1    ->  partition 0  offset 30
key=user-1    ->  partition 0  offset 31
key=user-4    ->  partition 1  offset 33
key=user-1    ->  partition 0  offset 32
key=user-2    ->  partition 0  offset 33
key=user-1    ->  partition 0  offset 34
key=user-2    ->  partition 0  offset 35
key=user-4    ->  partition 1  offset 34
key=user-4    ->  partition 1  offset 35
key=user-3    ->  partition 1  offset 36
key=user-2    ->  partition 0  offset 36
key=user-4    ->  partition 1  offset 37
key=user-3    ->  partition 1  offset 38
key=user-2    ->  partition 0  offset 37
key=user-1    ->  partition 0  offset 38
key=user-4    ->  partition 1  offset 39
key=user-3    ->  partition 1  offset 40
sent 20 events to topic 'clicks'
~~~
</details>

`Kafka` consumer groups allow multiple consumers to share the processing workload.

There are two important rules:
1. Within a consumer group, each partition can be assigned to only one consumer at a time.
2. Different consumer groups independently consume the same topic.

Whenever a consumer joins or leaves a group, Kafka can trigger a rebalance (this is why we have now an output in `terminal 3`). During rebalancing, partitions are reassigned between the remaining consumers.

![keys-partition-architecture](./images/keys-partition-architecture.png)
<center style="font-style: italic;">AI Generated illustration of the architecture</center>
</details>

> ### 4. Offsets: where a group continues
> 
> Stop all consumers (`Ctrl+C`).
> 
> *Predict:* you send 20 more events. Where
> does `dashboard` continue when `A` comes back?
> 
> ```bash
> python3 producer.py
> 
> docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
>   --describe --group dashboard
> ```
> 
> Per partition: `CURRENT-OFFSET` (where the group is), `LOG-END-OFFSET` (the
> end of the log) and `LAG` (the difference). Start `A` again and check your
> prediction. Then replay the whole topic for `billing`, and start `X` again:
> 
> ```bash
> docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
>   --group billing --topic clicks --reset-offsets --to-earliest --execute
> ```
> 
> > **The committed offset is the group's position in the log, stored in
> > Kafka.** A consumer continues where its group stopped; the lag says how
> > far behind it is. Reading deletes nothing: a reset (or a new group) reads
> > the history again, e.g. to fill a new service or to reprocess after a bug
> > fix, without affecting other groups.

<details>
<summary style="font-weight: bold;"><i>Predict:</i> you send 20 more events. Where does <code>dashboard</code> continue when <code>A</code> comes back?</summary>

**Output** for `docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group dashboard`:
~~~text
Consumer group 'dashboard' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
dashboard       clicks          1          50              59              9               -               -               -
dashboard       clicks          0          50              61              11              -               -               -
~~~

#### Expectation:
*Consumer A* continues from where it was left, it does not start automatically from the begining.

#### Observation:
Command: `python3 consumer.py --group dashboard --name A`.
~~~text
[A] joined group 'dashboard' on topic 'clicks' (Ctrl+C to stop)
[A] assigned partitions: [0, 1]
[A] partition 0 offset   50  key=user-1   seq=1
[A] partition 0 offset   51  key=user-2   seq=3
[A] partition 0 offset   52  key=user-1   seq=4
[A] partition 0 offset   53  key=user-1   seq=5
[A] partition 0 offset   54  key=user-1   seq=7
[A] partition 0 offset   55  key=user-2   seq=8
[A] partition 0 offset   56  key=user-1   seq=9
[A] partition 0 offset   57  key=user-2   seq=10
[A] partition 0 offset   58  key=user-2   seq=14
[A] partition 0 offset   59  key=user-2   seq=17
[A] partition 0 offset   60  key=user-1   seq=18
[A] partition 1 offset   50  key=user-3   seq=2
[A] partition 1 offset   51  key=user-4   seq=6
[A] partition 1 offset   52  key=user-4   seq=11
[A] partition 1 offset   53  key=user-4   seq=12
[A] partition 1 offset   54  key=user-3   seq=13
[A] partition 1 offset   55  key=user-4   seq=15
[A] partition 1 offset   56  key=user-3   seq=16
[A] partition 1 offset   57  key=user-4   seq=19
[A] partition 1 offset   58  key=user-3   seq=20
[A] revoked partitions:  [0, 1]
[A] stopped after 20 events
~~~

#### Expectation&mdash;reseting `billing`:
*X* starts reading the topic from the begining, rather than from the previous position.

#### Observation:
Command: `python3 consumer.py --group billing --name X`.
~~~text
[X] joined group 'billing' on topic 'clicks' (Ctrl+C to stop)
[X] assigned partitions: [0, 1]
[X] partition 0 offset    0  key=user-1   seq=1
[X] partition 0 offset    1  key=user-2   seq=3
[X] partition 0 offset    2  key=user-1   seq=4
[X] partition 0 offset    3  key=user-1   seq=5
[X] partition 0 offset    4  key=user-1   seq=7
[X] partition 0 offset    5  key=user-2   seq=8
[X] partition 0 offset    6  key=user-1   seq=9
[X] partition 0 offset    7  key=user-2   seq=10
[X] partition 0 offset    8  key=user-2   seq=14
[X] partition 0 offset    9  key=user-2   seq=17
[X] partition 0 offset   10  key=user-1   seq=18
[X] partition 0 offset   11  key=user-1   seq=1
[X] partition 0 offset   12  key=user-2   seq=3
[X] partition 0 offset   13  key=user-1   seq=4
[X] partition 0 offset   14  key=user-1   seq=5
[X] partition 0 offset   15  key=user-1   seq=7
[X] partition 0 offset   16  key=user-2   seq=8
[X] partition 0 offset   17  key=user-1   seq=9
[X] partition 0 offset   18  key=user-2   seq=10
[X] partition 0 offset   19  key=user-2   seq=14
[X] partition 0 offset   20  key=user-2   seq=17
[X] partition 0 offset   21  key=user-1   seq=18
[X] partition 0 offset   22  key=None     seq=7
[X] partition 0 offset   23  key=None     seq=10
[X] partition 0 offset   24  key=None     seq=11
[X] partition 0 offset   25  key=None     seq=14
[X] partition 0 offset   26  key=None     seq=15
[X] partition 0 offset   27  key=None     seq=16
[X] partition 0 offset   28  key=user-1   seq=1
[X] partition 0 offset   29  key=user-2   seq=3
[X] partition 0 offset   30  key=user-1   seq=4
[X] partition 0 offset   31  key=user-1   seq=5
[X] partition 0 offset   32  key=user-1   seq=7
[X] partition 0 offset   33  key=user-2   seq=8
[X] partition 0 offset   34  key=user-1   seq=9
[X] partition 0 offset   35  key=user-2   seq=10
[X] partition 0 offset   36  key=user-2   seq=14
[X] partition 0 offset   37  key=user-2   seq=17
[X] partition 0 offset   38  key=user-1   seq=18
[X] partition 0 offset   39  key=user-1   seq=1
[X] partition 0 offset   40  key=user-2   seq=3
[X] partition 0 offset   41  key=user-1   seq=4
[X] partition 0 offset   42  key=user-1   seq=5
[X] partition 0 offset   43  key=user-1   seq=7
[X] partition 0 offset   44  key=user-2   seq=8
[X] partition 0 offset   45  key=user-1   seq=9
[X] partition 0 offset   46  key=user-2   seq=10
[X] partition 0 offset   47  key=user-2   seq=14
[X] partition 0 offset   48  key=user-2   seq=17
[X] partition 0 offset   49  key=user-1   seq=18
[X] partition 0 offset   50  key=user-1   seq=1
[X] partition 0 offset   51  key=user-2   seq=3
[X] partition 0 offset   52  key=user-1   seq=4
[X] partition 0 offset   53  key=user-1   seq=5
[X] partition 0 offset   54  key=user-1   seq=7
[X] partition 0 offset   55  key=user-2   seq=8
[X] partition 0 offset   56  key=user-1   seq=9
[X] partition 0 offset   57  key=user-2   seq=10
[X] partition 0 offset   58  key=user-2   seq=14
[X] partition 0 offset   59  key=user-2   seq=17
[X] partition 0 offset   60  key=user-1   seq=18
[X] partition 1 offset    0  key=user-3   seq=2
[X] partition 1 offset    1  key=user-4   seq=6
[X] partition 1 offset    2  key=user-4   seq=11
[X] partition 1 offset    3  key=user-4   seq=12
[X] partition 1 offset    4  key=user-3   seq=13
[X] partition 1 offset    5  key=user-4   seq=15
[X] partition 1 offset    6  key=user-3   seq=16
[X] partition 1 offset    7  key=user-4   seq=19
[X] partition 1 offset    8  key=user-3   seq=20
[X] partition 1 offset    9  key=user-3   seq=2
[X] partition 1 offset   10  key=user-4   seq=6
[X] partition 1 offset   11  key=user-4   seq=11
[X] partition 1 offset   12  key=user-4   seq=12
[X] partition 1 offset   13  key=user-3   seq=13
[X] partition 1 offset   14  key=user-4   seq=15
[X] partition 1 offset   15  key=user-3   seq=16
[X] partition 1 offset   16  key=user-4   seq=19
[X] partition 1 offset   17  key=user-3   seq=20
[X] partition 1 offset   18  key=None     seq=1
[X] partition 1 offset   19  key=None     seq=2
[X] partition 1 offset   20  key=None     seq=3
[X] partition 1 offset   21  key=None     seq=4
[X] partition 1 offset   22  key=None     seq=5
[X] partition 1 offset   23  key=None     seq=6
[X] partition 1 offset   24  key=None     seq=8
[X] partition 1 offset   25  key=None     seq=9
[X] partition 1 offset   26  key=None     seq=12
[X] partition 1 offset   27  key=None     seq=13
[X] partition 1 offset   28  key=None     seq=17
[X] partition 1 offset   29  key=None     seq=18
[X] partition 1 offset   30  key=None     seq=19
[X] partition 1 offset   31  key=None     seq=20
[X] partition 1 offset   32  key=user-3   seq=2
[X] partition 1 offset   33  key=user-4   seq=6
[X] partition 1 offset   34  key=user-4   seq=11
[X] partition 1 offset   35  key=user-4   seq=12
[X] partition 1 offset   36  key=user-3   seq=13
[X] partition 1 offset   37  key=user-4   seq=15
[X] partition 1 offset   38  key=user-3   seq=16
[X] partition 1 offset   39  key=user-4   seq=19
[X] partition 1 offset   40  key=user-3   seq=20
[X] partition 1 offset   41  key=user-3   seq=2
[X] partition 1 offset   42  key=user-4   seq=6
[X] partition 1 offset   43  key=user-4   seq=11
[X] partition 1 offset   44  key=user-4   seq=12
[X] partition 1 offset   45  key=user-3   seq=13
[X] partition 1 offset   46  key=user-4   seq=15
[X] partition 1 offset   47  key=user-3   seq=16
[X] partition 1 offset   48  key=user-4   seq=19
[X] partition 1 offset   49  key=user-3   seq=20
[X] partition 1 offset   50  key=user-3   seq=2
[X] partition 1 offset   51  key=user-4   seq=6
[X] partition 1 offset   52  key=user-4   seq=11
[X] partition 1 offset   53  key=user-4   seq=12
[X] partition 1 offset   54  key=user-3   seq=13
[X] partition 1 offset   55  key=user-4   seq=15
[X] partition 1 offset   56  key=user-3   seq=16
[X] partition 1 offset   57  key=user-4   seq=19
[X] partition 1 offset   58  key=user-3   seq=20
[X] revoked partitions:  [0, 1]
[X] stopped after 120 events

~~~
</details>