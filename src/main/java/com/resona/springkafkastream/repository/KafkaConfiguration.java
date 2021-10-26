package com.resona.springkafkastream.repository;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST_ADDR);
        return new KafkaAdmin(configs);
    }

    @Bean
    public KafkaAdmin.NewTopics appTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(QUOTES_TOPIC).build(),
                TopicBuilder.name(LEVERAGE_PRICE_TOPIC).compact().build(),
                TopicBuilder.name(APPL_STOCKS_TOPIC).build(),
                TopicBuilder.name(GOOGL_STOCKS_TOPIC).build(),
                TopicBuilder.name(ALL_OTHER_STOCKS_TOPIC).build()
        );
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = defaultStreamsConfigs();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "quote-stream");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-quotes-stream-group");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> log.info("State transition from " + oldState + " to " + newState));
    }

    public Map<String, Object> defaultStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST_ADDR);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        return props;
    }

    // TOPICS
    public final static String QUOTES_TOPIC = "stock-quotes";
    public final static String LEVERAGE_PRICE_TOPIC = "leverage-prices";
    public final static String APPL_STOCKS_TOPIC = "appl-stocks-topic";
    public final static String GOOGL_STOCKS_TOPIC = "googl-stocks-topic";
    public final static String ALL_OTHER_STOCKS_TOPIC = "all-other-stocks-topic";

    public static final String KAFKA_HOST_ADDR = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    public static final Map<String, String> SERDE_CONFIG =
            Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    SCHEMA_REGISTRY_URL);
}
