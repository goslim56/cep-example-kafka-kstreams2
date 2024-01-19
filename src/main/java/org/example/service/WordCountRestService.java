package org.example.service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.example.model.SearchWord;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
public class WordCountRestService {

  private static final Serde<String> STRING_SERDE = Serdes.String();

//
  private final KafkaProducer kafkaProducer;
//
//  private final InteractiveQueryService interactiveQueryService;
//
//  @GetMapping("/count/local/{word}")
//  public Long getLocalWordCount(@PathVariable String word) {
//    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
//    ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
//        .store(StoreQueryParameters.fromNameAndType("word-counts", QueryableStoreTypes.keyValueStore()));
//    return counts.get(word);
//  }
//
//  @GetMapping("/count/inter/{word}")
//  public Long getInterWordCount(@PathVariable String word) {
//    HostInfo hostInfo = interactiveQueryService.getHostInfo("word-counts", word, STRING_SERDE.serializer());
//    if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
//      KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
//      ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
//          .store(StoreQueryParameters.fromNameAndType("word-counts", QueryableStoreTypes.keyValueStore()));
//      return counts.get(word);
//    }
//    else {
//      return 9999L;
//    }
//  }
//
//  @PostMapping("/message")
//  public void addMessage(@RequestBody String message) {
//    kafkaProducer.sendMessage(message);
//  }

//  @PostMapping("/message2")
//  public void addMessage(@RequestBody Message message) {
//    kafkaProducer.sendMessage2(message.message);
//  }

  @PostMapping("/message2")
  public void addMessage(@RequestBody SearchWord searchWord) {
    kafkaProducer.sendMessage2(searchWord);
  }
}