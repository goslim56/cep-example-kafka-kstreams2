package org.example.service.processorapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.example.model.CountWord;
import org.example.model.SearchWord;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WordCountProcessor implements Processor<String, SearchWord, Long, CountWord> {
    private ProcessorContext<Long, CountWord> context;
    private WindowStore<String, Long> windowStore;

    @Override
    public void init(ProcessorContext<Long, CountWord> context) {
        log.info("WordCountProcessorAPIStateful Start!!");
        this.context = context;
        windowStore = context.getStateStore("window-store");
        context.schedule(Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME, this::punctuate); //스케줄러 등록

    }

    private void punctuate(long timestamp) {
        long oneMinute = TimeUnit.MINUTES.toMillis(1);
        long windowStartTimestamp = timestamp - (timestamp % oneMinute);
        log.info("WordCountProcessor punctuate scheduler Start!!: {}/{}", context.taskId(), windowStartTimestamp);
        KeyValueIterator<Windowed<String>, Long> windowedCountIterator = windowStore.fetchAll(windowStartTimestamp, windowStartTimestamp + oneMinute);
        while (windowedCountIterator.hasNext()) {
            KeyValue<Windowed<String>, Long> windowedCount = windowedCountIterator.next();
            String word = windowedCount.key.key();
            Long count = windowedCount.value;
            long winStart = windowedCount.key.window().start();
            long windEnd = windowedCount.key.window().end();
            log.info("sink key/value/win_start/win_end: {}/{}/{}/{}", windowedCount.key.key(), windowedCount.value, windowedCount.key.window().start(), windowedCount.key.window().end());
            CountWord countWord = new CountWord(word, count, winStart, windEnd);
            context.forward(new Record<>(winStart, countWord, timestamp));
        }
    }


    @Override
    public void process(Record<String, SearchWord> record) {
        long oneMinute = TimeUnit.MINUTES.toMillis(1);
        long rowTime = record.timestamp();
//            long rowTime = dateToTimestamp(record.value().getTimestamp());
//            String[] words = record.value().getWord().toLowerCase().split(" ");
        String word = record.value().getWord();
        long windowStartTimestamp = rowTime - (rowTime % oneMinute);
        if (word.isBlank()) return;
        try {
            Long wordCount = windowStore.fetch(word, windowStartTimestamp);
            Long newValue = (wordCount == null) ? 1L : wordCount + 1;
            windowStore.put(word, newValue, windowStartTimestamp);
            log.info("windowStore put word/count : {}/{} ({})", word, newValue, windowStartTimestamp);
        } catch (Exception e) {
            log.error("process Exception: {}", e.getMessage());
        }
    }

    private Long dateToTimestamp(String date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss.SSS");
            Date parsedDate = dateFormat.parse(date);
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            return timestamp.getTime();
        } catch (Exception e) { //this generic but you can control another types of exception
            return 0L;
        }
    }


    //+---------------------------------------+
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