package org.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class CommonConfig {
  @Bean
  public RestTemplate restTemplate() {
    return new RestTemplate();
  }
  public static final String PROCESSOR_API_SUFFIX = "processorapi";
}
