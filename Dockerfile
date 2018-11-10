FROM maven:3.5-jdk-8 as BUILD

ADD pom.xml /tmp
RUN mvn -f /tmp/pom.xml verify clean --fail-never

ADD src /tmp/src
RUN mvn -f /tmp/pom.xml verify

FROM frolvlad/alpine-oraclejdk8:slim

COPY --from=BUILD /tmp/target/*.jar /app.jar

ENTRYPOINT ["java","-jar","/app.jar"]