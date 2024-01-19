package org.example.model.serde;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.example.model.CountWord;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class CountWordSerde extends WrapperSerde<CountWord> {
    public CountWordSerde() {
        super(new JsonSerializer<>(),
                new JsonDeserializer<>(CountWord.class));
    }
}