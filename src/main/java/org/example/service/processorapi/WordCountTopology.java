package org.example.service.processorapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.example.model.SearchWord;
import org.example.model.CountWord;
import org.example.model.WordRanking;
import org.example.model.serde.CountWordSerde;
import org.example.model.serde.SearchWordSerde;
import org.example.model.serde.WordRankingSerde;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Slf4j
public class WordCountTopology {
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<SearchWord> SEARCH_WORD_SERDE = new SearchWordSerde();
    private static final Serde<CountWord> COUNT_WORD_SERDE = new CountWordSerde();
    private static final Serde<WordRanking> WORD_RANKING_SERDE = new WordRankingSerde();


    @Autowired
    void buildPipeline(KafkaStreamsConfiguration defaultKafkaStreamsConfigProcessorApi) {

        Topology topology = new Topology();

        topology.addSource("Source", STRING_SERDE.deserializer(), SEARCH_WORD_SERDE.deserializer(), "input-processor-topic")
                .addProcessor("Process", WordCountProcessor::new, "Source")
                .addStateStore(Stores.windowStoreBuilder(
                        Stores.persistentTimestampedWindowStore("window-store", Duration.ofDays(1), Duration.ofMinutes(1), false),
                        STRING_SERDE,
                        LONG_SERDE), "Process")
                .addSink("Sink", "word-cnt-topic", LONG_SERDE.serializer(), COUNT_WORD_SERDE.serializer(), "Process")
                .addSource("Source2", LONG_SERDE.deserializer(), COUNT_WORD_SERDE.deserializer(), "word-cnt-topic")
                .addProcessor("Process2", AggregationProcessor::new, "Source2")
                .addStateStore(Stores.windowStoreBuilder(
                        Stores.persistentTimestampedWindowStore("aggregation-count-store", Duration.ofDays(1), Duration.ofMinutes(1), false),
                        STRING_SERDE,
                        LONG_SERDE), "Process2")
                .addSink("Sink2", "word-ranking-topic", LONG_SERDE.serializer(), WORD_RANKING_SERDE.serializer(), "Process2");

        KafkaStreams streams = new KafkaStreams(topology, new StreamsConfig(defaultKafkaStreamsConfigProcessorApi.asProperties()));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("WordCountProcessorAPI Shutdown success :)");
                streams.close();
            } catch (Exception e) {
                log.info("WordCountProcessorAPI Shutdown fail T_T");
            }
        }));
    }


}