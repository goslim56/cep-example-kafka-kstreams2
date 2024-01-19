package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.service.processorapi.AggregationProcessor;

import java.util.ArrayList;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WordRanking {
    private Ranking ranking; // ex) "maple" | "랩업하는법" | "캐시" | "메이플",
    private Long windowStartTimestamp; // ex) "2020–08–28T09:20:26.187"
    private Long windowEndTimestamp; // ex) "2020–08–28T09:20:26.187"

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class RankWord {
        private String words;
        private Long count;
        private String date;
        public RankWord (AggregationProcessor.TempCountWord tempCountWord) {
            if (tempCountWord == null) {
                words = null;
                count = null;
                date = null;
            } else {
                words = tempCountWord.getWord();
                count = tempCountWord.getCount();
                date = tempCountWord.getDate();
            }
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Ranking {
        @JsonProperty("1")
        private RankWord first;
        @JsonProperty("2")
        private RankWord second;
        @JsonProperty("3")
        private RankWord third;
        @JsonProperty("4")
        private RankWord fourth;
        @JsonProperty("5")
        private RankWord Fifth;


    }
}