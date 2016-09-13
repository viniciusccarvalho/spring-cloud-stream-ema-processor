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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;

/**
 * @author Vinicius Carvalho
 */
public class FluxTest {

	private Flux<String> emitter;
	private ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setup() {
		emitter = Flux.interval(Duration.ofMillis(100)).map(tick -> {
			return Quote.random();
		}).map(quote -> {
			String payload = null;
			try {
				payload = mapper.writeValueAsString(quote);
			}
			catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			return payload;
		});
	}

	@Test
	public void testSimpleSubscriber() throws Exception {
		final CountDownLatch latch = new CountDownLatch(10);
		emitter.subscribe(s -> {
			System.out.println(s);
			latch.countDown();
		});
		latch.await();
	}

	@Test
	public void testGroupBy() throws Exception {
		final Map<String, Map<String, Double>> averages = new HashMap<>();
		Cancellation cancellation = emitter.map(s -> {
			Quote payload = null;
			try {
				payload = mapper.readValue(s, Quote.class);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			return payload;
		}).groupBy(Quote::getSymbol).flatMap(grp -> {
			return grp.buffer(Duration.ofMillis(1000));
		}).subscribe(maps -> {
			double totalBid = 0.0;
			double totalAsk = 0.0;
			for (Quote quote : maps) {
				totalAsk += quote.getAsk();
				totalBid += quote.getBid();
			}
			Map<String, Double> entry = new HashMap<String, Double>();
			entry.put("ask", totalAsk / maps.size());
			entry.put("bid", totalAsk / maps.size());
			averages.put(maps.get(0).getSymbol() + "_" + System.currentTimeMillis(),
					entry);
		});
		Thread.sleep(3000L);
		cancellation.dispose();
		System.out.println(averages);

	}

	@Test
	public void testGroupByReduce() throws Exception {
		emitter.map(s -> {
			Quote payload = null;
			try {
				payload = mapper.readValue(s, Quote.class);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			return payload;
		}).window(Duration.ofMillis(1000))
				.flatMap(w -> w.groupBy(Quote::getSymbol).flatMap(group -> group
						.reduce(new ExponentialMovingAverage(), (ewma, quote) -> {
							ewma.compute(quote.getBid());
							return ewma;
						}).map(ewma1 -> {
							return new GroupedMovingAverage(group.key(),
									ewma1.getCurrentValue());
						})))
				.subscribe(System.out::println)

		;
		Thread.sleep(3000L);
	}

	static class Quote {
		static String[] symbols = new String[] { "AAPL", "GOOGL", "AMZN", "MSFT" };

		static Random random = new Random();
		private String symbol;
		private Double ask;
		private Double bid;
		private Long timestamp;

		public static Quote random() {
			String symbol = symbols[random.nextInt(symbols.length)];
			Quote quote = new Quote();
			quote.setSymbol(symbol);
			quote.setBid(random.nextDouble());
			quote.setAsk(random.nextDouble());
			quote.setTimestamp(System.currentTimeMillis());
			return quote;
		}

		public String getSymbol() {
			return symbol;
		}

		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}

		public Double getAsk() {
			return ask;
		}

		public void setAsk(Double ask) {
			this.ask = ask;
		}

		public Double getBid() {
			return bid;
		}

		public void setBid(Double bid) {
			this.bid = bid;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}
	}
}
