# ShardingJH

ShardingJH is a distributed database project that relies on Spring Boot, Hibernate and SQLite. It features P2P routing, gossip-based node discovery and sharding between multiple SQLite instances.

## Requirements

- Java 17 or later
- SQLite installed locally
- (Optional) Maven if you prefer not to use the provided wrapper

## Build and Run

1. Clone this repository.
2. Ensure Java 17 and SQLite are available on your PATH.
3. Build the project using the Maven wrapper:
   ```bash
   ./mvnw clean package -DskipTests
   ```
4. Choose a server profile (`server1`, `server2`, `server3` or `server4`) and run the application with the helper script:
   ```bash
   ./run-app.sh server1
   ```
   Profiles contain individual port and routing settings stored in the `src/main/resources` directory.

### Manual Run

If you prefer to start the jar manually, run:
```bash
java -jar target/*.jar --spring.profiles.active=server1
```

## Deployment to AWS

The project contains a GitHub Actions workflow (`.github/workflows/github-actions-ec2.yml`) that deploys the packaged jar to multiple EC2 servers. Each server corresponds to a different profile. The deployment script also inserts the RabbitMQ password into `application.properties` before running `run-app.sh` remotely.

## Division of Work

- **JiDung** – Encryption, P2P routing, Sharding strategy, MVCC and roll back, Static file replication, RabbitMQ product update sync, Frontend, AWS Deployment, Spring Boot framework, SQLite establishment
- **Haopeng** – Gossip, HeartBeat messaging, Dynamic hash allocating

