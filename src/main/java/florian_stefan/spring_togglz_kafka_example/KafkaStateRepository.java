package florian_stefan.spring_togglz_kafka_example;

import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.togglz.core.util.FeatureStateStorageWrapper.featureStateForWrapper;
import static org.togglz.core.util.FeatureStateStorageWrapper.wrapperForFeatureState;

import com.google.gson.GsonBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.togglz.core.Feature;
import org.togglz.core.repository.FeatureState;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.util.FeatureStateStorageWrapper;


public class KafkaStateRepository implements AutoCloseable, StateRepository {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaStateRepository.class);

  private final FeatureStateConsumer featureStateConsumer;
  private final FeatureStateProducer featureStateProducer;
  private final Map<String, FeatureStateStorageWrapper> featureStates;

  public KafkaStateRepository(String bootstrapServers, String featureStateTopic, Duration pollingTimeout) {
    featureStateConsumer = new FeatureStateConsumer(bootstrapServers, featureStateTopic, pollingTimeout);
    featureStateProducer = new FeatureStateProducer(bootstrapServers, featureStateTopic);
    featureStates = new ConcurrentHashMap<>();

    featureStateConsumer.start();
  }

  @Override
  public void close() {
    featureStateConsumer.close();
    featureStateProducer.close();
  }

  @Override
  public FeatureState getFeatureState(Feature feature) {
    if (featureStateConsumer.isRunning()) {
      FeatureStateStorageWrapper storageWrapper = featureStates.get(feature.name());

      if (storageWrapper != null) {
        return featureStateForWrapper(feature, storageWrapper);
      } else {
        LOG.warn("Could not find featureState for given feature - fallback to default value.");
        return null;
      }
    } else {
      LOG.warn("FeatureStateConsumer not running anymore - fallback to default value.");
      return null;
    }
  }

  @Override
  public void setFeatureState(FeatureState featureState) {
    featureStateProducer.send(featureState.getFeature(), wrapperForFeatureState(featureState));
  }

  class FeatureStateConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch countDownLatch;
    private final String featureStateTopic;
    private final Duration pollingTimeout;

    private volatile boolean running;

    FeatureStateConsumer(String bootstrapServers, String featureStateTopic, Duration pollingTimeout) {
      Properties properties = new Properties();
      properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(GROUP_ID_CONFIG, "feature-state-consumer-" + randomUUID());
      properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
      properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
      properties.setProperty(MAX_POLL_RECORDS_CONFIG, "200");

      this.consumer = new KafkaConsumer<>(properties);
      this.countDownLatch = new CountDownLatch(1);
      this.featureStateTopic = featureStateTopic;
      this.pollingTimeout = pollingTimeout;

      running = false;
    }

    void start() {
      if (running) {
        LOG.info("FeatureStateConsumer has already been started.");
      } else {
        synchronized (this) {
          LOG.info("Starting to start FeatureStateConsumer.");
          new Thread(this::run).start();
          LOG.info("Successfully started FeatureStateConsumer.");
          running = true;
        }
      }
    }

    void close() {
      if (running) {
        synchronized (this) {
          try {
            LOG.info("Starting to close FeatureStateConsumer.");
            consumer.wakeup();
            countDownLatch.await();
            running = false;
            LOG.info("Successfully closed FeatureStateConsumer.");
          } catch (InterruptedException e) {
            LOG.error("An error occurred while closing FeatureStateConsumer.", e);
          }
        }
      } else {
        LOG.info("FeatureStateConsumer has already been closed.");
      }
    }

    boolean isRunning() {
      return running;
    }

    private void run() {
      consumer.subscribe(singleton(featureStateTopic));

      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(pollingTimeout);

          processRecords(records);

          consumer.commitSync();
        }
      } catch (WakeupException e) {
        LOG.info("Received shutdown signal.");
      } catch (Exception e) {
        LOG.error("An error occurred while processing feature states.", e);
      } finally {
        shutdown();
      }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
      for (ConsumerRecord<String, String> record : records) {
        String featureName = record.key();
        String featureStateAsString = record.value();

        try {
          LOG.info("Starting to process state of feature {}.", featureName);
          FeatureStateStorageWrapper storageWrapper = deserialize(featureStateAsString);
          LOG.info("Successfully deserialized state of feature {}.", featureName);
          featureStates.put(featureName, storageWrapper);
          LOG.info("Successfully processed state of feature {}.", featureName);
        } catch (Exception e) {
          LOG.error("An error occurred while processing state of feature {}.", featureName);
        }
      }
    }

    private FeatureStateStorageWrapper deserialize(String featureStateAsString) {
      return new GsonBuilder().create().fromJson(featureStateAsString, FeatureStateStorageWrapper.class);
    }

    private void shutdown() {
      try {
        LOG.info("Starting to close KafkaConsumer.");
        consumer.close();
        LOG.info("Successfully closed KafkaConsumer.");
      } catch (Exception e) {
        LOG.error("An error occurred while closing KafkaConsumer!");
      } finally {
        countDownLatch.countDown();
      }
    }
  }

  class FeatureStateProducer {

    private final KafkaProducer<String, String> producer;
    private final String featureStateTopic;

    FeatureStateProducer(String bootstrapServers, String featureStateTopic) {
      Properties properties = new Properties();
      properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ACKS_CONFIG, "all");

      this.producer = new KafkaProducer<>(properties);
      this.featureStateTopic = featureStateTopic;
    }

    void close() {
      try {
        LOG.info("Starting to close KafkaProducer.");
        producer.close();
        LOG.info("Successfully closed KafkaProducer.");
      } catch (Exception e) {
        LOG.error("An error occurred while closing KafkaProducer!");
      }
    }

    void send(Feature feature, FeatureStateStorageWrapper storageWrapper) {
      String featureName = feature.name();

      try {
        LOG.info("Starting to update state of feature {}.", featureName);
        String featureStateAsString = serialize(storageWrapper);
        LOG.info("Successfully serialized state of feature {}.", featureName);
        producer.send(new ProducerRecord<>(featureStateTopic, featureName, featureStateAsString));
        LOG.info("Successfully updated state of feature {}.", featureName);
      } catch (Exception e) {
        LOG.error("An error occurred while updating state of feature {}.", featureName);
        throw new KafkaStateRepositoryException(feature, e);
      }
    }

    private String serialize(FeatureStateStorageWrapper storageWrapper) {
      return new GsonBuilder().create().toJson(storageWrapper);
    }

  }

}
