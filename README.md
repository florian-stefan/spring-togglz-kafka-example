# Spring Togglz Kafka Example

This application demonstrates the usage of Togglz with a Kafka based state repository. The example can be started with
Docker Compose by running the command `docker-compose up -d` in the root folder of the project. This will create four
Docker containers: one container for running a Zookeeper instance, one container for running a Kafka broker and two
containers for running two instances of a simple example application. The first instance of the example application
will be bound to the port 8080 and the second instance to the port 8081.

The example application provides the HTTP endpoint `GET http://localhost:<PORT>/features/<FEATURE>` for retrieving the
current state of a feature switch and the HTTP endpoint `GET http://localhost:<PORT>/features/<FEATURE>/toggle` for
toggling the feature switch. In both cases, the feature switches are identified by their name given as path parameter.

Since the example applications contains the three features `FEATURE_A`, `FEATURE_B` and `FEATURE_C`, the following ...

###### Commands

* `docker-compose up -d`
* `docker-compose up -d --build`
* `docker-compose down`
* `docker-compose down -v`
* `docker ps -a`
* `docker stop <CONTAINER>`
* `docker start <CONTAINER>`
* `docker logs <CONTAINER>`

###### TODO

* Documentation (e.g. consumer group / consumer lags / compacted topic)
