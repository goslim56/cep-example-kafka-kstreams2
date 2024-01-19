package org.example.model.serde;

import lombok.Data;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.example.model.SearchWord;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class SearchWordSerde extends WrapperSerde<SearchWord> {
    public SearchWordSerde() {
        super(new JsonSerializer<>(),
                new JsonDeserializer<>(SearchWord.class));
    }
}