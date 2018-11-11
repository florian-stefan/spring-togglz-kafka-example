# Spring Togglz Kafka Example

This application demonstrates the usage of Togglz with a Kafka based state repository. The example can be started with
Docker Compose by running the command `docker-compose up -d` in the root folder of the project. This will create four
Docker containers: one container for running a Zookeeper instance, one container for running a Kafka broker and two
containers for running two instances of a simple example application. The first instance of the example application
will be bound to the port 8080 and the second instance to the port 8081.

The example application provides the HTTP endpoint `GET http://localhost:<PORT>/features/<FEATURE>` for retrieving the
current state of a feature and the HTTP endpoint `GET http://localhost:<PORT>/features/<FEATURE>/toggle` for toggling
the feature. In both cases, the features are identified by their name given as path parameter. The application defines
features `FEATURE_A`, `FEATURE_B` as well as `FEATURE_C` and, therefore, provides the following HTTP endpoints:

* http://localhost:8080/features/feature_a (for retrieving the state of `FEATURE_A` from the first application instance)
* http://localhost:8080/features/feature_a/toggle (for toggling `FEATURE_A` on the first application instance)
* http://localhost:8081/features/feature_a (for retrieving the state of `FEATURE_A` from the second application instance)
* http://localhost:8081/features/feature_a/toggle (for toggling `FEATURE_B` on the second application instance)
* http://localhost:8080/features/feature_b (for retrieving the state of `FEATURE_B` from the first application instance)
* http://localhost:8080/features/feature_b/toggle (for toggling `FEATURE_B` on the first application instance)
* http://localhost:8081/features/feature_b (for retrieving the state of `FEATURE_B` from the second application instance)
* http://localhost:8081/features/feature_b/toggle (for toggling `FEATURE_B` on the second application instance)
* http://localhost:8080/features/feature_c (for retrieving the state of `FEATURE_C` from the first application instance)
* http://localhost:8080/features/feature_c/toggle (for toggling `FEATURE_C` on the first application instance)
* http://localhost:8081/features/feature_c (for retrieving the state of `FEATURE_C` from the second application instance)
* http://localhost:8081/features/feature_c/toggle (for toggling `FEATURE_C` on the second application instance)


###### Commands

* `docker-compose up -d`
* `docker-compose up -d --build`
* `docker-compose down`
* `docker-compose down -v`
* `docker ps -a`
* `docker stop <CONTAINER>`
* `docker start <CONTAINER>`
* `docker logs <CONTAINER>`