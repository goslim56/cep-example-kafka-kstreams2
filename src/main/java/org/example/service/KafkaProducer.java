package org.example.service;

import org.example.model.SearchWord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@Component
public class KafkaProducer {

//    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, SearchWord> kafkaTemplate;

//    public void sendMessage(String message) {
//        kafkaTemplate.send("input-topic", message)
//                .whenComplete((result, ex) -> {
//                    if (ex == null) {
//                        log.info("Message sent to topic: {}", message);
//                    } else {
//                        log.error("Failed to send message", ex);
//                    }
//                });
//    }

    public void sendMessage2(SearchWord searchWord) {
        kafkaTemplate.send("input-processor-topic", searchWord.getWord(), searchWord)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent to topic: {}", searchWord);
                    } else {
                        log.error("Failed to send message", ex);
                    }
                });
    }


}