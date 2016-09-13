package org.springframework.cloud.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableBinding(Processor.class)
@EnableConfigurationProperties(ExponentialMovingAverageProperties.class)
public class FluxEmaProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(FluxEmaProcessorApplication.class, args);
	}

	@Bean
	public ObjectMapper mapper() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper;
	}
}
