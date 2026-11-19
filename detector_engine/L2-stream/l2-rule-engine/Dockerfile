FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
RUN mvn -B clean package

FROM flink:2.2.1-java17
COPY --from=build /build/target/l2-rule-engine-1.0.0-all.jar /opt/flink/usrlib/l2-rule-engine.jar
