package florian_stefan.spring_togglz_kafka_example;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RestController;
import org.togglz.core.manager.EnumBasedFeatureProvider;
import org.togglz.core.spi.FeatureProvider;

@RestController
@SpringBootApplication
public class Application {

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  private static final String BOOTSTRAP_SERVERS = "kafka:9092";
  private static final String TOGGLZ_TOPIC = "feature-states";

  public static void main(String[] args) {
    createTogglzTopicIfNecessary();

    SpringApplication.run(Application.class, args);
  }

  @Bean
  @SuppressWarnings("unchecked")
  public FeatureProvider featureProvider() {
    return new EnumBasedFeatureProvider(Features.class);
  }

  @Bean(destroyMethod = "close")
  public KafkaStateRepository getStateRepository() {
    return KafkaStateRepository.create(BOOTSTRAP_SERVERS, TOGGLZ_TOPIC, Duration.ofMillis(200));
  }

  private static void createTogglzTopicIfNecessary() {
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    try (AdminClient adminClient = KafkaAdminClient.create(properties)) {
      if (adminClient.listTopics().names().get().contains(TOGGLZ_TOPIC)) {
        LOG.info("Topic {} has already been created.", TOGGLZ_TOPIC);
      } else {
        LOG.info("Starting to create topic {}.", TOGGLZ_TOPIC);
        adminClient.createTopics(singleton(newTopic())).all().get();
        LOG.info("Successfully created topic {}.", TOGGLZ_TOPIC);
      }
    } catch (Exception e) {
      LOG.error("An error occurred while creating topic.", e);
    }
  }

  private static NewTopic newTopic() {
    Map<String, String> configuration = new HashMap<>();
    configuration.put("cleanup.policy", "compact");
    return new NewTopic(TOGGLZ_TOPIC, 3, (short) 1).configs(configuration);
  }

}
