package florian_stefan.spring_togglz_kafka_example;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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

  private KafkaStateRepository(
      String bootstrapServers,
      String inboundTopic,
      String outboundTopic,
      Duration pollingTimeout,
      Duration initializationTimeout
  ) {
    featureStateConsumer = new FeatureStateConsumer(bootstrapServers, inboundTopic, pollingTimeout, initializationTimeout);
    featureStateProducer = new FeatureStateProducer(bootstrapServers, outboundTopic);
    featureStates = new ConcurrentHashMap<>();

    featureStateConsumer.start();
  }

  public static KafkaStateRepository create(
      String bootstrapServers,
      String featureStateTopic,
      Duration pollingTimeout,
      Duration initializationTimeout
  ) {
    return new KafkaStateRepository(
        requireNonNull(bootstrapServers),
        requireNonNull(featureStateTopic),
        requireNonNull(featureStateTopic),
        requireNonNull(pollingTimeout),
        requireNonNull(initializationTimeout)
    );
  }

  public static KafkaStateRepository createWithInboundAndOutboundTopic(
      String bootstrapServers,
      String inboundTopic,
      String outboundTopic,
      Duration pollingTimeout,
      Duration initializationTimeout
  ) {
    return new KafkaStateRepository(
        requireNonNull(bootstrapServers),
        requireNonNull(inboundTopic),
        requireNonNull(outboundTopic),
        requireNonNull(pollingTimeout),
        requireNonNull(initializationTimeout)
    );
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

  public long consumerLag() {
    return featureStateConsumer.consumerLag();
  }

  class FeatureStateConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final String inboundTopic;
    private final Duration pollingTimeout;
    private final Map<TopicPartition, Long> offsets;
    private final CountDownLatch initializationLatch;
    private final Duration initializationTimeout;
    private final CountDownLatch shutdownLatch;

    private volatile boolean running;
    private volatile long consumerLag;

    FeatureStateConsumer(
        String bootstrapServers,
        String inboundTopic,
        Duration pollingTimeout,
        Duration initializationTimeout
    ) {
      Properties properties = new Properties();
      properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(GROUP_ID_CONFIG, "feature-state-consumer-" + randomUUID());
      properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
      properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

      this.consumer = new KafkaConsumer<>(properties);
      this.inboundTopic = inboundTopic;
      this.pollingTimeout = pollingTimeout;
      this.offsets = new ConcurrentHashMap<>();
      this.initializationLatch = new CountDownLatch(1);
      this.initializationTimeout = initializationTimeout;
      this.shutdownLatch = new CountDownLatch(1);

      running = false;
      consumerLag = Long.MAX_VALUE;
    }

    void start() {
      if (running) {
        LOG.info("FeatureStateConsumer has already been started.");
      } else {
        synchronized (this) {
          if (running) {
            try {
              LOG.info("Starting to start FeatureStateConsumer.");
              new Thread(this::run).start();
              LOG.info("Successfully started FeatureStateConsumer.");
              running = true;
              initializationLatch.await(initializationTimeout.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
              throw new RuntimeException("An error occurred while awaiting initialization.", e);
            }
          } else {
            LOG.info("FeatureStateConsumer has already been started.");
          }
        }
      }
    }

    void close() {
      if (running) {
        synchronized (this) {
          if (running) {
            try {
              LOG.info("Starting to close FeatureStateConsumer.");
              consumer.wakeup();
              shutdownLatch.await();
              running = false;
              LOG.info("Successfully closed FeatureStateConsumer.");
            } catch (InterruptedException e) {
              LOG.error("An error occurred while closing FeatureStateConsumer.", e);
            }
          } else {
            LOG.info("FeatureStateConsumer has already been closed.");
          }
        }
      } else {
        LOG.info("FeatureStateConsumer has already been closed.");
      }
    }

    boolean isRunning() {
      return running;
    }

    long consumerLag() {
      return consumerLag;
    }

    private void run() {
      consumer.subscribe(singleton(inboundTopic));

      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(pollingTimeout);

          processRecords(records);

          updateConsumerLag();

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
          updatePartitionOffset(record.partition(), record.offset());
        } catch (Exception e) {
          throw new RuntimeException("An error occurred while processing state of feature " + featureName + ".", e);
        }
      }
    }

    private FeatureStateStorageWrapper deserialize(String featureStateAsString) {
      return new GsonBuilder().create().fromJson(featureStateAsString, FeatureStateStorageWrapper.class);
    }

    private void updatePartitionOffset(int partition, long offset) {
      offsets.put(new TopicPartition(inboundTopic, partition), offset);
    }

    private void updateConsumerLag() {
      consumerLag = accumulateEndOffsets();

      if (consumerLag < 1) {
        initializationLatch.countDown();
      }
    }

    private long accumulateEndOffsets() {
      AtomicLong accumulator = new AtomicLong(0L);

      consumer.endOffsets(consumer.assignment()).forEach((topicPartition, endOffset) -> {
        long oldValue = accumulator.get();
        long partitionOffset = endOffset - offsets.getOrDefault(topicPartition, 0L) - 1;
        accumulator.set(oldValue + partitionOffset);
      });

      return accumulator.get();
    }

    private void shutdown() {
      try {
        LOG.info("Starting to close KafkaConsumer.");
        consumer.close();
        LOG.info("Successfully closed KafkaConsumer.");
      } catch (Exception e) {
        LOG.error("An error occurred while closing KafkaConsumer!", e);
      } finally {
        shutdownLatch.countDown();
      }
    }
  }

  class FeatureStateProducer {

    private final KafkaProducer<String, String> producer;
    private final String outboundTopic;

    FeatureStateProducer(String bootstrapServers, String outboundTopic) {
      Properties properties = new Properties();
      properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ACKS_CONFIG, "all");

      this.producer = new KafkaProducer<>(properties);
      this.outboundTopic = outboundTopic;
    }

    void close() {
      try {
        LOG.info("Starting to close KafkaProducer.");
        producer.close();
        LOG.info("Successfully closed KafkaProducer.");
      } catch (Exception e) {
        LOG.error("An error occurred while closing KafkaProducer!", e);
      }
    }

    void send(Feature feature, FeatureStateStorageWrapper storageWrapper) {
      String featureName = feature.name();

      try {
        LOG.info("Starting to update state of feature {}.", featureName);
        String featureStateAsString = serialize(storageWrapper);
        LOG.info("Successfully serialized state of feature {}.", featureName);
        producer.send(buildRecord(featureName, featureStateAsString), buildCallback(featureName));
      } catch (Exception e) {
        LOG.error("An error occurred while updating state of feature {}.", featureName, e);
      }
    }

    private String serialize(FeatureStateStorageWrapper storageWrapper) {
      return new GsonBuilder().create().toJson(storageWrapper);
    }

    private ProducerRecord<String, String> buildRecord(String featureName, String featureStateAsString) {
      return new ProducerRecord<>(outboundTopic, featureName, featureStateAsString);
    }

    private Callback buildCallback(String featureName) {
      return (metadata, exception) -> {
        if (exception == null) {
          LOG.info("Successfully updated state of feature {}.", featureName);
        } else {
          LOG.error("An error occurred while updating state of feature {}.", featureName, exception);
        }
      };
    }

  }

}
