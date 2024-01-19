package org.example.model.serde;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.example.model.WordRanking;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class RankingSerde extends WrapperSerde<WordRanking.Ranking> {
    public RankingSerde() {
        super(new JsonSerializer<>(),
                new JsonDeserializer<>(WordRanking.Ranking.class));
    }
}