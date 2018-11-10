package florian_stefan.spring_togglz_kafka_example;

import org.togglz.core.Feature;

public class KafkaStateRepositoryException extends RuntimeException {

  private final Feature feature;

  KafkaStateRepositoryException(Feature feature, Throwable cause) {
    super("Failed to process state of feature " + feature.name() + ".", cause);
    this.feature = feature;
  }

  public Feature getFeature() {
    return feature;
  }

}
