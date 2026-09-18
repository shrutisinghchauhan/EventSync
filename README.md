# Solving the Dual Write Problem in Distributed Systems

A practical implementation of multiple reliable event-driven design patterns to solve the **Dual Write Problem** using **Kafka**, **PostgreSQL**, **Debezium**, and **Microservices Architecture**.

---

# Introduction

In distributed systems and microservice architectures, services often need to perform two operations together:

1. Write data to the database  
2. Publish an event/message to a message broker like Kafka  

This creates the **Dual Write Problem**.

The main challenge with dual writes is maintaining consistency between the database and the message broker. Since both operations happen independently, failures during either operation can lead to inconsistent system states, such as:
- Data being stored in the database but the event not being published
- Event being published but the database transaction failing

This repository demonstrates different approaches to solving the Dual Write Problem using reliable event-driven design patterns.

<br>

![Dual Write Problem](/Images/Dual_Write_problem.png)

<br>

---

# Design Patterns Implemented

## 1. Transactional Outbox Pattern

The service writes both the business data and an outbox event into the same database transaction. A separate process then reads the outbox table and publishes events to Kafka, ensuring reliable event delivery and maintaining consistency.

<br>

![Transactional Outbox Pattern](/Images/Transactional%20Outbox%20Pattern.png)

<br>

---

## 2. Listen To Yourself Pattern

The service publishes events to Kafka and also consumes its own events to update internal state or trigger further processing. This pattern enables event-driven workflows while helping maintain consistency across services.

<br>

![Listen To Yourself Pattern](/Images/Listen%20To%20YourSelf%20Pattern.png)

<br>

---

## 3. Transactional Log Tailing Pattern

Instead of directly publishing events, database transaction logs (WAL/binlogs) are monitored using tools like Debezium. Changes are captured directly from the database logs and streamed to Kafka, ensuring reliable and consistent event publishing.

<br>

![Transactional Log Tailing Pattern](/Images/Transactional%20Log%20Tailing%20Pattern.png)

<br>

---

# Goal of This Project

This project aims to demonstrate and simulate how modern distributed systems solve consistency problems between database operations and asynchronous event publishing using different architectural patterns and messaging strategies.

# Database Schema

Below is the database schema used in the project architecture.

![Database Schema](./Images/SCHEMA.png)

# Poller Work Flow 

<img src="./Images/workflow1.png" alt="Workflow 1" width="400">

<img src="./Images/workflow2.jpg" alt="Workflow 2" width="400">


# UI Simulation

[![Watch the UI Simulation](./Images/thumbnail.png)](https://drive.google.com/file/d/16VK7HRab0EsdAbO8uWNyxW2-bpG2himC/view?usp=sharing)


# Installation Guide



## 1. Create Docker Containers



Before starting the project, make sure Docker and Docker Compose are installed on your system.



Go into each service directory and build the containers using Docker Compose.



```bash

docker compose up --build -d

```



Repeat this step for all the required services/containers in the project.



---



## 2. Start All Services



After all Docker containers are created successfully, move to the root directory of the project and run:



```bash

chmod +x start_all_services.sh

./start_all_services.sh

```



This script will automatically start all required backend services, pollers, consumers, and supporting infrastructure.



---



## 3. Debezium Connector Setup



After the Debezium container starts successfully, attach the connector manually during installation.



Example:



```bash

curl -X POST http://localhost:8083/connectors \

-H "Content-Type: application/json" \

-d @connector.json

```



Make sure:



* Kafka is running

* Kafka Connect is running

* PostgreSQL/MySQL database is accessible

* `connector.json` contains the correct database configuration



You can verify the connector using:



```bash

curl http://localhost:8083/connectors

```



---



## 4. Verify Running Containers



To check whether all containers are running:



```bash

docker ps

```



To check logs of a specific container:



```bash

docker logs <container_name>

```



---



## 5. Stop All Containers



To stop all running containers:



```bash

docker compose down

```



# Nomenclature

## 1. Kafka

### Topics
- `Orders_1___Transactional_Outbox_Pattern`
- `Orders_2___Listen_To_Yourself_Pattern`
- `Orders_3___Transactional_Log_Tailing`

<br>

---

## 2. Order Service Producers

### Services
- `order_service_1-backend-1`
- `order_service_2-backend-1`

These services are responsible for:
- Writing order data to PostgreSQL
- Publishing or generating events for Kafka
- Demonstrating different solutions to the Dual Write Problem

<br>

---

## 3. Database

### Database Name
- `postgres_outbox`

### Tables

#### Order
Stores business order data for all implemented patterns.

**Pattern Types**
- `Transactional_Outbox`
- `Listen_To_Yourself`
- `Transactional_Log_Tailing`

#### Outbox_Transactional_Outbox
Stores events/messages for the Transactional Outbox Pattern before they are published to Kafka.

#### Outbox_Listen_To_Yourself
Stores events/messages used in the Listen To Yourself Pattern workflow.

#### ProcessedEvent
Used for idempotency in the Listen To Yourself Pattern to prevent duplicate event processing.

> Schema and table structures can be explored using Prisma Studio.

<br>

---

## 4. Pollers

Pollers continuously monitor outbox tables and publish pending events to Kafka.

### Transactional Outbox Pattern


- `poller_1_transactional_outbox-backend-1`
- `poller_2_transactional_outbox-backend-1`

### Listen To Yourself Pattern


- `poller_1_listen_to_yourself-backend-1`
- `poller_2_listen_to_yourself-backend-1`

<br>

---

## 5. Debezium

Debezium is used to implement the Transactional Log Tailing Pattern by capturing database changes directly from PostgreSQL transaction logs and streaming them to Kafka.

### Setup Debezium Connector

#### Register Connector

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
--data @register-postgres.json
```

#### Delete Connector

```bash
curl -X DELETE http://localhost:8083/connectors/postgres-connector
```

#### View All Connectors

```bash
curl http://localhost:8083/connectors
```

### Debezium ↔ Kafka Integration

The integration between Debezium and Kafka is configured inside:

```text
docker-compose.yml
```

<br>

---

# Overall Architecture Components

```text
Client Request
      ↓
Order Service
      ↓
PostgreSQL Database
      ↓
 ┌───────────────────────────────┐
 │ Dual Write Solution Patterns  │
 └───────────────────────────────┘
      ↓
 ├── Transactional Outbox Poller
 ├── Listen To Yourself Poller
 └── Debezium CDC
      ↓
Kafka Topics
      ↓
Consumers / Downstream Services
```

### Built with passion, persistence, and lots of debugging ☕

#### Crafted by **Pranav Jarande**

⭐ Feel free to fork, improve, and add your own innovations to this project.
If you found this repository useful, consider giving it a star!

</div>
