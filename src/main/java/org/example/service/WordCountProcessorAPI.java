package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class WordCountProcessorAPI {
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();


    @Autowired
    void buildPipeline(KafkaStreamsConfiguration defaultKafkaStreamsConfigProcessorApi) {

        Topology topology = new Topology();

        topology.addSource("Source", STRING_SERDE.deserializer(), STRING_SERDE.deserializer(), "input-processor-topic")
                .addProcessor("Process", WordCountProcessorAPIStateful::new, "Source")
                .addStateStore(Stores.windowStoreBuilder(
                        Stores.persistentTimestampedWindowStore("window-store", Duration.ofDays(1), Duration.ofMinutes(1), false),
                        STRING_SERDE,
                        LONG_SERDE), "Process")
                .addSink("Sink", "output-processor-topic", STRING_SERDE.serializer(), LONG_SERDE.serializer(), "Process");

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


    public static class WordCountProcessorAPIStateful implements Processor<String, String, String, Long> {
        private ProcessorContext<String, Long> context;
        private WindowStore<String, Long> windowStore;

        @Override
        public void init(ProcessorContext<String, Long> context) {
            log.info("WordCountProcessorAPIStateful Start!!");
            this.context = context;
            windowStore = context.getStateStore("window-store");
            context.schedule(Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME, this::punctuate); //스케줄러 등록

        }

        private void punctuate(long timestamp) {
            long oneMinute = TimeUnit.MINUTES.toMillis(1);
            long windowStartTimestamp = timestamp - (timestamp % oneMinute);
            log.info("punctuate scheduler Start!!: {}/{}", context.taskId(), windowStartTimestamp);
            KeyValueIterator<Windowed<String>, Long> windowedCountIterator = windowStore.fetchAll(windowStartTimestamp, windowStartTimestamp + oneMinute);
            while (windowedCountIterator.hasNext()) {
                KeyValue<Windowed<String>, Long> windowedCount = windowedCountIterator.next();
                log.info("sink key/value/win_start/win_end: {}/{}/{}/{}", windowedCount.key.key(), windowedCount.value,windowedCount.key.window().start(),windowedCount.key.window().end());
                context.forward(new Record<>(windowedCount.key.key(),windowedCount.value, timestamp));
            }
        }


        @Override
        public void process(Record<String, String> record) {
            long oneMinute = TimeUnit.MINUTES.toMillis(1);
            long rowTime = record.timestamp();
            String[] words = record.value().toLowerCase().split(" ");
            long windowStartTimestamp = rowTime - (rowTime % oneMinute);
            for (String word : words) {
                if (word.isBlank()) continue;
                try {
                    Long wordCount = windowStore.fetch(word, windowStartTimestamp);
                    Long newValue = (wordCount == null) ? 1L : wordCount + 1;
                    windowStore.put(word, newValue, windowStartTimestamp);
                    log.info("windowStore put word/count : {}/{} ({})", word,newValue,windowStartTimestamp);
                } catch (Exception e) {
                    log.error("process Exception: {}", e.getMessage());
                }
            }
        }


        //  +---------------------------------------+
        //|  key  | start time | end time | count |
        //+-------+------------+----------+-------+
        //|   A   |     10     |    20    |   4   |
        //+-------+------------+----------+-------+
        //|   B   |     10     |    20    |   4   |
        //+-------+------------+----------+-------+
        //|   C   |     10     |    20    |   4   |
        //+-------+------------+----------+-------+
        //|   A   |     20     |    30    |   3   |
        //+-------+------------+----------+-------+
        //|   A   |     30     |    40    |   2   |
        //+-------+------------+----------+-------+
        //|   A   |     40     |    50    |   4   |
        //+---------------------------------------+
        public void close() {
            windowStore.close();
        }
    }
}