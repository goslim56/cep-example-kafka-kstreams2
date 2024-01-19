package org.example.service.processorapi;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
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
import org.example.model.WordRanking;
import org.jetbrains.annotations.NotNull;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AggregationProcessor implements Processor<Long, CountWord, Long, WordRanking> {
    private ProcessorContext<Long, WordRanking> context;
    private WindowStore<String, Long> windowStore;

    @Override
    public void init(ProcessorContext<Long, WordRanking> context) {
        log.info("AggregationProcessor Start!!");
        this.context = context;
        windowStore = context.getStateStore("aggregation-count-store");
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, this::punctuate); //스케줄러 등록

    }

    private void punctuate(long timestamp) {
        long oneMinute = TimeUnit.MINUTES.toMillis(1);
        long oneMinuteAgo = timestamp - oneMinute;
        long windowStartTimestamp = oneMinuteAgo - (oneMinuteAgo % oneMinute);
        long windowEndTimestamp = windowStartTimestamp + oneMinute;
        log.info("AggregationProcessor punctuate scheduler Start!!: {}/{}", context.taskId(), windowStartTimestamp);
        TreeSet<TempCountWord> tempCountWordSet = new TreeSet<>();
        ArrayList<TempCountWord> tempCountWords = new ArrayList<>(tempCountWordSet);

        KeyValueIterator<Windowed<String>, Long> windowedCountIterator = windowStore.fetchAll(windowStartTimestamp, windowEndTimestamp);
        while (windowedCountIterator.hasNext()) {
            KeyValue<Windowed<String>, Long> windowedCount = windowedCountIterator.next();

            String word = windowedCount.key.key();
            Long count = windowedCount.value;
            tempCountWords.add(new TempCountWord(word, count));
            tempCountWords.sort(TempCountWord::compareTo);
            while (tempCountWords.size() > 5) {
                tempCountWords.remove(5);
            }
            log.info("tempCountWordSet.size: {} ", tempCountWords.size());
//            if (tempCountWordSet.size() > 5) {
//                tempCountWordSet.pollLast();
//            }
            log.info("AggregationProcessor add word/count/win_start/win_end: {}/{}/{}/{}", windowedCount.key.key(), windowedCount.value, windowedCount.key.window().start(), windowedCount.key.window().end());

        }
//        ArrayList<TempCountWord> tempCountWords = new ArrayList<>(tempCountWordSet);
        System.out.println("tempCountWords = " + tempCountWords);
        if (tempCountWords.isEmpty()) return;
        tempCountWords.forEach(System.out::println);
        while (tempCountWords.size() < 5) {
            tempCountWords.add(null);
        }
        WordRanking wordRanking = new WordRanking(
                new WordRanking.Ranking(
                        new WordRanking.RankWord(tempCountWords.get(0)),
                        new WordRanking.RankWord(tempCountWords.get(1)),
                        new WordRanking.RankWord(tempCountWords.get(2)),
                        new WordRanking.RankWord(tempCountWords.get(3)),
                        new WordRanking.RankWord(tempCountWords.get(4))
                ),
                timestampToDate(windowStartTimestamp),
                timestampToDate(windowEndTimestamp)

        );
        context.forward(new Record<>(windowStartTimestamp, wordRanking, timestamp));
    }


    @Override
    public void process(Record<Long, CountWord> record) {
        long oneMinute = TimeUnit.MINUTES.toMillis(1);
        long rowTime = record.timestamp();
        Long aggregationTime = record.key();
        String word = record.value().getWord();
        Long count = record.value().getCount();
//        if (word.isBlank()) return;
        try {
            Long wordCount = windowStore.fetch(word, aggregationTime);
            Long newCount = (wordCount == null) ? count : wordCount + count;
            windowStore.put(word, newCount, record.key());
            log.info("AggregationProcessor windowStore put word/count/part : {}/{}/{} ({})", word, newCount, context.taskId(),aggregationTime);
        } catch (Exception e) {
            log.error("process Exception: {}", e.getMessage());
        }
    }

    private Long dateToTimestamp(String date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss.SSS");
            Date parsedDate = dateFormat.parse(date);
            Timestamp timestamp = new Timestamp(parsedDate.getTime());
            return timestamp.getTime();
        } catch (Exception e) { //this generic but you can control another types of exception
            return 0L;
        }
    }

    private String timestampToDate(Long timestamp) {
        try {
            Date date = new Date(timestamp);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.KOREA);
            return sdf.format(new Date(timestamp));
        } catch (Exception e) { //this generic but you can control another types of exception
            System.out.println("e123123 = " + e);
            return "날짜버그";
        }
    }

    public void close() {
        windowStore.close();
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class TempCountWord implements Comparable<TempCountWord> {
        private String word; // ex) "maple" | "랩업하는법" | "캐시" | "메이플",
        private Long count; // ex) "2020–08–28T09:20:26.187"

        @Override
        public int compareTo(@NotNull TempCountWord o) {
            return Long.valueOf(o.count- count).intValue();
        }
    }
}