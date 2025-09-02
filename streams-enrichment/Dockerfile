# =================
# 1. Build Stage
# =================
FROM maven:3.9.6-eclipse-temurin-17 AS build

# Set the working directory
WORKDIR /app

# Copy the pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy the source code and build the JAR
COPY src ./src
# Use package -DskipTests to create the JAR without running tests
RUN mvn package -DskipTests


# =================
# 2. Final Stage
# =================
# Use a lightweight JRE image to run the application
FROM eclipse-temurin:17-jre

# Set the working directory
WORKDIR /app

# ---- FIX: Install netcat here ----
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd postgresql-client && rm -rf /var/lib/apt/lists/*

# Copy the JAR from the 'build' stage and rename it to app.jar
COPY --from=build /app/target/streams-enrichment-1.0-SNAPSHOT.jar /app/app.jar

# Copy the necessary startup scripts
COPY start-app.sh .
COPY wait-for-it.sh .

# Make the scripts executable
RUN chmod +x start-app.sh wait-for-it.sh

# Set the entrypoint to run the script
CMD ["./wait-for-it.sh"]
