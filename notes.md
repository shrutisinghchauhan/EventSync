# TODO -:
1. Nomenclature -- after scalability                                         --- Done
2. Design poller for Listen to yourself - PUBLISH EVENTS TO NEW KAFKA TOPIC  --- Done 
3. Check if cleanup happens properly                                         --- Done 
4. Testing of ltu Poller                                                     --- Done 
5. Consume events by same order_Service to add data to orders table          --- Done
6. Check simultaneous consumption of events                                  --- Done
7. Idempotencty Solved - Consumer side                                       --- Done
8. Filter Orders using : Pattern_Type (Column)                               --- Done
9. Create Routes in Database for tlt                                         --- Done
10. Deploy Container for Debezium                                            --- Done
11. Read WAL from Debezium along with filter (Transactional_Log_Tailing)     --- Done
12. Publish Events from Debezium                                             --- Done
13. Socket connection in Consumer for getting an event                       --- Done 
14. Socket connection with Order_Service                                     --- Done  
15. UI                                                                       --- Done
16. Consumer                                                                 --- Done
17. Create a script to Restart and Install the project                       --- Done
18. Poller logs on UI                                                        --- Done

#Command to restart containers 
chmod +x start_all_services.sh
./start_all_services.sh

Study deployment of Kafka

# Nomenclature 
1. Kafka 
   Topics - Orders_1___Transactional_Outbox_Pattern
          - Orders_2___Listen_To_Yourself_Pattern
          - Orders_3___Transactional_Log_Tailing
  
2. Order_Service_Producers 
   Services - order_service_1-backend-1
            - order_service_2-backend-1
    
3. Database - 1 Database in both services Container: postgres_outbox
   Tables -  Order ( Pattern_Type - Transactional_Outbox , Listen_To_Yourself , Transactional_Log_Tailing)
          -  Outbox_Transactional_Outbox
          -  Outbox_Listen_To_yourself
          -  ProcessedEvent (LTU IDEMPOTENCY)
   - (Can get Schema from Prisma Studio)
  
4. Poller 
   - Transactional Outbox Pattern : 
        1.  poller_1_transactional_outbox-backend-1
        2.  poller_2_transactional_outbox-backend-1
   - Listen to Yourself Pattern :
        1.  poller_1_listen_to_yourself-backend-1
        2.  poller_2_listen_to_yourself-backend-1
   
5. Debezium 
      - setup debezium
 - ## CONNECTORS : ( DB <--> Debezium )
    - add connector - curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" --data @register-postgres.json
    - delete connector - curl -X DELETE http://localhost:8083/connectors/postgres-connector
    - See all connectors - curl http://localhost:8083/connectors
 -  ## CONNECTOR : (Debezium <---> Kafka)
    - Done in Docker-compose.yml



Difference between DockerFile and dockercompose.yml
Docker Network 

Working of Kafka 
why brokers: ["kafka:29092"], 

Start Commands
PSQL 
- New user : docker-compose up -d
- Start existing : docker start postgres_outbox

Order Service : docker-compose up --build


# Commands -:
docker ps -a
docker start container_name

docker-compose down --remove-orphans
docker-compose build --no-cache
docker-compose up

--- removes docker container 
docker compose down
docker compose up -d --build

DATABASE :
To see prisma tables 
- npx prisma studio

# Restart : Once all containers exist :

1. Kafka
 - docker start kafka 

2. Database
 - docker start postgres_outbox

3. Order Service 
   docker start order_service_1-backend-1
   docker start order_service_2-backend-1

4. Start the pollers 
   docker start poller_1-backend-1
   docker start poller_2-backend-2


# Installations from beginning 
1. Database_Setup
2. Kafka Setup
      - Create topics in Kafka 
3. Order_Service setup
4. Start Pollers 