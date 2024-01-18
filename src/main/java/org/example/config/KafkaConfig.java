package org.example.config;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.example.config.CommonConfig.PROCESSOR_API_SUFFIX;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;
  @Value(value = "${spring.kafka.application-server}")
  private String applicationServerAddress;
  @Value(value = "${spring.kafka.streams.state.dir}")
  private String stateStoreLocation;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  KafkaStreamsConfiguration kStreamsConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(APPLICATION_ID_CONFIG, "streams-app");
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(APPLICATION_SERVER_CONFIG, applicationServerAddress); // for interactive query service
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // configure the state location to allow tests to use clean state for every run
    props.put(STATE_DIR_CONFIG, stateStoreLocation);

    return new KafkaStreamsConfiguration(props);
  }
  @Bean(name = "defaultKafkaStreamsConfigProcessorApi")
  KafkaStreamsConfiguration kStreamsConfigProcessorAPI() {
    Map<String, Object> props = new HashMap<>();
    props.put(APPLICATION_ID_CONFIG, "streams-app-" + PROCESSOR_API_SUFFIX);
    props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(APPLICATION_SERVER_CONFIG, applicationServerAddress); // for interactive query service
    props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST.toString());
    props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");

    // configure the state location to allow tests to use clean state for every run
    props.put(STATE_DIR_CONFIG, stateStoreLocation+PROCESSOR_API_SUFFIX);

    return new KafkaStreamsConfiguration(props);
  }


  @Bean
  NewTopic inputTopic() {
    return TopicBuilder.name("input-topic")
            .partitions(3)
            .replicas(2)
            .build();
  }

  @Bean
  NewTopic inputTopic2() {
    return TopicBuilder.name("input-processor-topic")
            .partitions(3)
            .replicas(2)
            .build();
  }
  @Bean
  NewTopic outputTopic2() {
    return TopicBuilder.name("output-processor-topic")
            .partitions(3)
            .replicas(2)
            .build();
  }
}