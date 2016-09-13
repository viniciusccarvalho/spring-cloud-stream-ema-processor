/*
 *  Copyright 2016 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.springframework.cloud.stream;

import java.io.IOException;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;

/**
 * @author Vinicius Carvalho
 */
@Component

public class ExponentialMovingAverageProcessor {

	private ObjectMapper mapper;
	private ExponentialMovingAverageProperties properties;

	@Autowired
	public ExponentialMovingAverageProcessor(ObjectMapper mapper,
			ExponentialMovingAverageProperties properties) {
		this.mapper = mapper;
		this.properties = properties;
	}

	@StreamListener
	@Output(Processor.OUTPUT)
	public Flux<GroupedMovingAverage> computeEWMA(
			@Input(Processor.INPUT) Flux<String> emitter) {
		return emitter.map(s -> {
			JsonNode payload = null;
			try {
				payload = mapper.readTree(s);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			return payload;
		}).window(Duration.ofSeconds(properties.getWindow()))
				.flatMap(w -> w.groupBy(
						jsonNode -> jsonNode.get(properties.getGroupKey()).asText())
						.flatMap(group -> group
								.reduce(new ExponentialMovingAverage(), (ewma, node) -> {
									ewma.compute(node.get(properties.getFieldName())
											.asDouble());
									return ewma;
								}).map(ewma1 -> {
									return new GroupedMovingAverage(group.key(),
											ewma1.getCurrentValue());
								})));
	}

}
