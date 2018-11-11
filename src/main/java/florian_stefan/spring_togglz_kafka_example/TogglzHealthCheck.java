package florian_stefan.spring_togglz_kafka_example;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class TogglzHealthCheck implements HealthIndicator {

  private final KafkaStateRepository stateRepository;

  public TogglzHealthCheck(KafkaStateRepository stateRepository) {
    this.stateRepository = stateRepository;
  }

  @Override
  public Health health() {
    return isHealthy() ? Health.up().build() : Health.down().build();
  }

  private boolean isHealthy() {
    return stateRepository.isRunning() && stateRepository.consumerLag() < 1;
  }

}
