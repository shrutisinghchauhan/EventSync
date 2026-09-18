# EventSync — Solving the Dual Write Problem

A working implementation of three reliable event-driven patterns that keep a
database and a message broker consistent: **transactional outbox**,
**listen-to-yourself**, and **log tailing via CDC**.

Built with Kafka, PostgreSQL, Debezium, and Node.js microservices.

---

## The problem

An order service typically needs to do two things at once:

1. Write the order to its database
2. Publish an `OrderPlaced` event to Kafka so other services react

Written as two sequential calls, this is quietly broken:

```
save order to Postgres     ← succeeds
publish event to Kafka     ← process crashes here
```

The order now exists, but inventory never decrements and no confirmation is
sent. Reverse the two calls and you get the opposite bug: an event announcing
an order that was never saved.

You cannot wrap both in a transaction, because Postgres and Kafka share no
transactional context. That is the **dual write problem**.

![Dual Write Problem](/Images/Dual_Write_problem.png)

---

## How the outbox pattern solves it

```mermaid
flowchart TD
    A[Client places order] --> B[Order Service]
    B --> C{Single DB transaction}
    C --> D[(orders table)]
    C --> E[(outbox table<br/>status = PENDING)]
    C -.->|both commit or<br/>neither does| F[Transaction boundary]

    E --> G[Poller claims batch<br/>FOR UPDATE SKIP LOCKED]
    G --> H[Mark PROCESSING<br/>commit]
    H --> I[Publish to Kafka]

    I -->|success| J[Mark PUBLISHED]
    I -->|failure| K[Reset to PENDING<br/>attempts + 1]
    K --> G

    J --> L[Kafka topic]
    L --> M[Consumer]
    M --> N{Seen this event<br/>before?}
    N -->|yes| O[Skip]
    N -->|no| P[(consumer DB write<br/>+ ProcessedEvent row)]
```

The order and the outbox row land in **one transaction**, so they are always
consistent with each other. Publishing happens separately, and because the
outbox row survives a crash, a failed publish is simply retried.

---

## Why two pollers don't collide

Two poller instances run concurrently against the same outbox table. Without
coordination they would both claim the same rows and publish duplicates.

```mermaid
sequenceDiagram
    participant P1 as Poller 1
    participant P2 as Poller 2
    participant DB as PostgreSQL
    participant K as Kafka

    P1->>DB: SELECT ... FOR UPDATE SKIP LOCKED (limit 10)
    DB-->>P1: rows 1-10 (locked)
    P2->>DB: SELECT ... FOR UPDATE SKIP LOCKED (limit 10)
    Note over DB,P2: rows 1-10 are locked —<br/>SKIP LOCKED skips them
    DB-->>P2: rows 11-20

    P1->>DB: UPDATE status = PROCESSING, COMMIT
    P2->>DB: UPDATE status = PROCESSING, COMMIT
    Note over P1,P2: locks released, work is disjoint

    P1->>K: publish rows 1-10
    P2->>K: publish rows 11-20
```

`SKIP LOCKED` makes a locked row **invisible** to the second query rather than
making it wait. The two pollers get disjoint batches and neither blocks the
other.

### Outbox row lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING: written in order transaction
    PENDING --> PROCESSING: claimed via SKIP LOCKED
    PROCESSING --> PUBLISHED: Kafka ack received
    PROCESSING --> PENDING: publish failed, attempts + 1
    PROCESSING --> PENDING: poller crashed, cleanup sweep
    PUBLISHED --> [*]
```

The claim is committed **before** the publish. That ordering is deliberate: it
guarantees at-least-once delivery rather than at-most-once. A crash after the
commit but before the publish leaves a row stranded in `PROCESSING`, which the
cleanup sweep returns to `PENDING`.

---

## At-least-once means consumers must be idempotent

Because a publish can succeed while the acknowledgement is lost, the same event
can arrive twice. The consumer therefore checks a `ProcessedEvent` table inside
the same transaction as its write:

```mermaid
flowchart LR
    A[Event arrives] --> B{Exists in<br/>ProcessedEvent?}
    B -->|yes| C[Acknowledge<br/>and skip]
    B -->|no| D[BEGIN]
    D --> E[Write business data]
    E --> F[Insert ProcessedEvent]
    F --> G[COMMIT]
    G --> H[Acknowledge]
```

Both writes are in one transaction, so a crash mid-way cannot leave an event
marked processed without its effect having landed.

---

## The three patterns

### 1. Transactional Outbox

Business data and event are written in one transaction; a separate poller
publishes the outbox rows to Kafka.

![Transactional Outbox Pattern](/Images/Transactional%20Outbox%20Pattern.png)

### 2. Listen To Yourself

The service publishes to Kafka first and consumes its own event to perform the
database write, inverting which system is the source of truth.

![Listen To Yourself Pattern](/Images/Listen%20To%20YourSelf%20Pattern.png)

### 3. Transactional Log Tailing (CDC)

No poller at all. Debezium reads the PostgreSQL write-ahead log — the same log
used for replication — and streams every committed change to Kafka. The
application code is unaware it is happening.

![Transactional Log Tailing Pattern](/Images/Transactional%20Log%20Tailing%20Pattern.png)

---

## Architecture

```mermaid
flowchart LR
    C[Client]

    subgraph Producers
        OS1[Order Service 1<br/>:3000]
        OS2[Order Service 2<br/>:3001]
    end

    subgraph Storage
        PG[(postgres_outbox<br/>wal_level = logical)]
    end

    subgraph Relays
        P1[Outbox Poller 1 :8000]
        P2[Outbox Poller 2 :8001]
        P3[LTY Poller 1 :8002]
        P4[LTY Poller 2 :8003]
        DBZ[Debezium CDC :8083]
    end

    subgraph Downstream
        CS[Consumer Service<br/>:9000]
        CDB[(postgres_consumer)]
    end

    C --> OS1 & OS2
    OS1 & OS2 --> PG
    PG --> P1 & P2 & P3 & P4 & DBZ
    P1 & P2 & P3 & P4 & DBZ --> KF[Kafka :9092]
    KF --> CS --> CDB
    KF --> UI[Kafka UI :8080]
```

---

## Results

| Metric | Value |
|---|---|
| Sustained throughput, outbox path | _TBD_ |
| End-to-end p95 latency | _TBD_ |
| Backlog recovery after 60s broker outage | _TBD_ |
| Events lost across induced failures | 0 |

Measured with `BATCH_SIZE=10`, `POLL_INTERVAL=3000ms`, two concurrent pollers.
To reproduce, drive load from the simulation UI and run `docker stop kafka`
mid-flight, then `docker start kafka` and time the drain.

---

## Database schema

![Database Schema](./Images/SCHEMA.png)

| Table | Purpose |
|---|---|
| `Order` | Business order data for all three patterns |
| `Outbox_Transactional_Outbox` | Pending events for the outbox pattern |
| `Outbox_Listen_To_Yourself` | Events for the listen-to-yourself workflow |
| `ProcessedEvent` | Idempotency keys so redelivery is safe |

Schema can be explored with Prisma Studio.

---

## Setup

**Prerequisites:** Docker Desktop, Node.js 18+, and a bash shell
(Git Bash on Windows).

### 1. Create the shared network

Every compose file joins an **external** network that nothing creates for you:

```bash
docker network create outbox-network
```

### 2. Create the environment files

Each service's `docker-compose.yaml` declares `env_file: .env`, but those files
are gitignored. Create them before building, or compose will refuse to start.

Each **poller**:

```env
CORS_ORIGIN=*
BATCH_SIZE=10
POLL_INTERVAL=3000
CLEANUP_INTERVAL=60000
SERVICE_NAME=poller_1_top
DATABASE_URL=postgresql://myuser:mypassword@postgres_outbox:5432/outbox_db
```

Each **order service**:

```env
CORS_ORIGIN=*
SERVICE_NAME=order_service_1
DATABASE_URL=postgresql://myuser:mypassword@postgres_outbox:5432/outbox_db
```

The **consumer service**:

```env
CORS_ORIGIN=*
SERVICE_NAME=consumer_service
DATABASE_URL=postgresql://myuser:mypassword@postgres_consumer:5432/consumer_db
```

> `DATABASE_URL` must use the **container name** (`postgres_outbox`), not
> `localhost` — inside a container `localhost` resolves to that container
> itself. Use port `5432` for both databases; `5433` is only the host-side
> mapping for the consumer database.

### 3. Databases and schema

```bash
cd DataBase_Setup
docker compose up -d
npm install
DATABASE_URL="postgresql://myuser:mypassword@localhost:5432/outbox_db" npx prisma migrate deploy
cd ..

cd Consumer_DB_Setup
docker compose up -d
npm install
DATABASE_URL="postgresql://myuser:mypassword@localhost:5433/consumer_db" npx prisma migrate deploy
cd ..
```

These run from the host, so they use `localhost` and the host-mapped ports.

### 4. Kafka and Debezium

```bash
cd Kafka && docker compose up -d && cd ..
sleep 40

cd "3. Transactional Log Tailing Pattern/debezium" && docker compose up -d && cd ../..
sleep 45

curl http://localhost:8083/connectors          # expect []

curl -i -X POST -H "Content-Type: application/json" \
  --data @"3. Transactional Log Tailing Pattern/debezium/register-postgres.json" \
  http://localhost:8083/connectors             # expect 201 Created
```

Delete the connector with:

```bash
curl -X DELETE http://localhost:8083/connectors/postgres-connector
```

### 5. Build the services

```bash
cd Order_Services_Producer/Order_Service_1 && docker compose up -d --build && cd ../..
cd Order_Services_Producer/Order_Service_2 && docker compose up -d --build && cd ../..
cd Consumer_Service && docker compose up -d --build && cd ..
cd "1. Transactional Outbox Pattern/Poller_1_Transactional_Outbox" && docker compose up -d --build && cd ../..
cd "1. Transactional Outbox Pattern/Poller_2_Transactional_Outbox" && docker compose up -d --build && cd ../..
cd "2. Listen to Yourself/Poller_1_Listen_To_Yourself" && docker compose up -d --build && cd ../..
cd "2. Listen to Yourself/Poller_2_Listen_To_Yourself" && docker compose up -d --build && cd ../..

docker ps        # expect 12 containers
```

### 6. Run the UI

```bash
cd Simulation && npm install && npm run dev
```

Open <http://localhost:5173>.

On later runs everything is already built, so:

```bash
bash start_all_services.sh
```

Stop everything with `docker compose down` in each service directory.

---

## Watching it work

| Where | What you see |
|---|---|
| <http://localhost:5173> | Place orders, switch patterns, inject failures |
| <http://localhost:8080> | Kafka UI — messages landing in topics |
| `docker logs -f poller_1_transactional_outbox-backend-1` | Claim → publish → retry cycle |

To see recovery in action, run `docker stop kafka` while orders are flowing,
keep placing orders, then `docker start kafka` and watch the backlog drain.

---

## Service map

| Service | Port | Role |
|---|---|---|
| Order Service 1 / 2 | 3000 / 3001 | Accept orders, write outbox rows |
| Outbox Pollers | 8000 / 8001 | Claim and publish outbox events |
| Listen-to-Yourself Pollers | 8002 / 8003 | Claim and publish LTY events |
| Consumer Service | 9000 | Idempotent downstream consumer |
| Kafka | 9092 | Message broker |
| Kafka UI | 8080 | Topic inspection |
| Debezium Connect | 8083 | CDC from the Postgres WAL |
| postgres_outbox | 5432 | Producer database |
| postgres_consumer | 5433 (host) → 5432 (container) | Consumer database |

### Kafka topics

- `Orders_1___Transactional_Outbox_Pattern`
- `Orders_2___Listen_To_Yourself_Pattern`
- `Orders_3___Transactional_Log_Tailing`
