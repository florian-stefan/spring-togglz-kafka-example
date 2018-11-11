package florian_stefan.spring_togglz_kafka_example;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.togglz.core.util.FeatureStateStorageWrapper.featureStateForWrapper;
import static org.togglz.core.util.FeatureStateStorageWrapper.wrapperForFeatureState;

import com.google.gson.GsonBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
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

  private final Map<String, FeatureStateStorageWrapper> featureStates;
  private final FeatureStateConsumer featureStateConsumer;
  private final FeatureStateProducer featureStateProducer;

  private KafkaStateRepository(Builder builder) {
    featureStates = new ConcurrentHashMap<>();
    featureStateConsumer = new FeatureStateConsumer(builder, featureStates::put);
    featureStateProducer = new FeatureStateProducer(builder);

    featureStateConsumer.start();
  }

  public static Builder builder() {
    return new Builder();
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

  public static class Builder {

    private String bootstrapServers;
    private String inboundTopic;
    private String outboundTopic;
    private Duration pollingTimeout;
    private Duration initializationTimeout;

    private Builder() {
    }

    public Builder bootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
      return this;
    }

    public Builder inboundTopic(String inboundTopic) {
      this.inboundTopic = inboundTopic;
      return this;
    }

    public Builder outboundTopic(String outboundTopic) {
      this.outboundTopic = outboundTopic;
      return this;
    }

    public Builder pollingTimeout(Duration pollingTimeout) {
      this.pollingTimeout = pollingTimeout;
      return this;
    }

    public Builder initializationTimeout(Duration initializationTimeout) {
      this.initializationTimeout = initializationTimeout;
      return this;
    }

    public KafkaStateRepository build() {
      return new KafkaStateRepository(this);
    }

  }

  static class FeatureStateConsumer {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final BiConsumer<String, FeatureStateStorageWrapper> updateHandler;
    private final String inboundTopic;
    private final Duration pollingTimeout;
    private final Map<TopicPartition, Long> offsets;
    private final CountDownLatch initializationLatch;
    private final Duration initializationTimeout;
    private final CountDownLatch shutdownLatch;

    private volatile boolean running;
    private volatile long consumerLag;

    FeatureStateConsumer(Builder builder, BiConsumer<String, FeatureStateStorageWrapper> updateHandler) {
      Properties properties = new Properties();
      properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, requireNonNull(builder.bootstrapServers));
      properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");

      this.kafkaConsumer = new KafkaConsumer<>(properties);
      this.updateHandler = updateHandler;
      this.inboundTopic = requireNonNull(builder.inboundTopic);
      this.pollingTimeout = requireNonNull(builder.pollingTimeout);
      this.offsets = new ConcurrentHashMap<>();
      this.initializationLatch = new CountDownLatch(1);
      this.initializationTimeout = requireNonNull(builder.initializationTimeout);
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
            LOG.info("FeatureStateConsumer has already been started.");
          } else {
            try {
              LOG.info("Starting to start FeatureStateConsumer.");
              new Thread(this::run).start();
              LOG.info("Successfully started FeatureStateConsumer.");
              running = true;
              initializationLatch.await(initializationTimeout.toMillis(), MILLISECONDS);
              LOG.info("Successfully initialized FeatureStateConsumer.");
            } catch (InterruptedException e) {
              throw new RuntimeException("An error occurred while awaiting initialization.", e);
            }
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
              kafkaConsumer.wakeup();
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
      assignConsumer();

      try {
        while (true) {
          ConsumerRecords<String, String> records = kafkaConsumer.poll(pollingTimeout);

          processRecords(records);

          updateConsumerLag();

          kafkaConsumer.commitSync();
        }
      } catch (WakeupException e) {
        LOG.info("Received shutdown signal.");
      } catch (Exception e) {
        LOG.error("An error occurred while processing feature states.", e);
      } finally {
        shutdown();
      }
    }

    private void assignConsumer() {
      LOG.info("Starting to retrieve partitions for topic {}.", inboundTopic);
      List<TopicPartition> topicPartitions = getTopicPartitions();
      LOG.info("Successfully retrieved topic partitions {}.", topicPartitions);

      kafkaConsumer.assign(topicPartitions);
      kafkaConsumer.seekToBeginning(topicPartitions);
    }

    private List<TopicPartition> getTopicPartitions() {
      List<TopicPartition> topicPartitions = new ArrayList<>();

      for (PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(inboundTopic)) {
        topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
      }

      return topicPartitions;
    }

    private void processRecords(ConsumerRecords<String, String> records) {
      for (ConsumerRecord<String, String> record : records) {
        String featureName = record.key();
        String featureStateAsString = record.value();

        try {
          LOG.info("Starting to process state of feature {}.", featureName);
          FeatureStateStorageWrapper storageWrapper = deserialize(featureStateAsString);
          LOG.info("Successfully deserialized state of feature {}.", featureName);
          updateHandler.accept(featureName, storageWrapper);
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
      LOG.info("Starting to update offset {} of partition {}.", offset, partition);
      offsets.put(new TopicPartition(inboundTopic, partition), offset);
      LOG.info("Successfully updated offset {} of partition {}.", offset, partition);
    }

    private void updateConsumerLag() {
      consumerLag = accumulateEndOffsets();

      if (consumerLag < 1) {
        initializationLatch.countDown();
      }
    }

    private long accumulateEndOffsets() {
      AtomicLong accumulator = new AtomicLong(0L);

      kafkaConsumer.endOffsets(kafkaConsumer.assignment()).forEach((topicPartition, endOffset) -> {
        if (endOffset > 0L) {
          long oldValue = accumulator.get();
          long partitionOffset = endOffset - offsets.getOrDefault(topicPartition, 0L) - 1L;
          accumulator.set(oldValue + partitionOffset);
        }
      });

      return accumulator.get();
    }

    private void shutdown() {
      try {
        LOG.info("Starting to close KafkaConsumer.");
        kafkaConsumer.close();
        LOG.info("Successfully closed KafkaConsumer.");
      } catch (Exception e) {
        LOG.error("An error occurred while closing KafkaConsumer!", e);
      } finally {
        shutdownLatch.countDown();
      }
    }
  }

  static class FeatureStateProducer {

    private final KafkaProducer<String, String> producer;
    private final String outboundTopic;

    FeatureStateProducer(Builder builder) {
      Properties properties = new Properties();
      properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, requireNonNull(builder.bootstrapServers));
      properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ACKS_CONFIG, "all");

      this.producer = new KafkaProducer<>(properties);
      this.outboundTopic = requireNonNull(builder.outboundTopic);
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
