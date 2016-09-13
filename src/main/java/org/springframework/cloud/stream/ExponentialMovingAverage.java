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

import static java.lang.Math.*;

/**
 * @author Vinicius Carvalho
 */
public class ExponentialMovingAverage {

	private static final int INTERVAL = 5;

	private static final double SECONDS_PER_MINUTE = 60.0;

	private static final int ONE_MINUTE = 1;

	public static final double M1_ALPHA = 1
			- exp(-INTERVAL / SECONDS_PER_MINUTE / ONE_MINUTE);

	private static final int FIVE_MINUTES = 5;

	public static final double M5_ALPHA = 1
			- exp(-INTERVAL / SECONDS_PER_MINUTE / FIVE_MINUTES);

	private static final int FIFTEEN_MINUTES = 15;

	public static final double M15_ALPHA = 1
			- exp(-INTERVAL / SECONDS_PER_MINUTE / FIFTEEN_MINUTES);

	private final double alpha;

	private Double oldValue;

	private Double currentValue;

	public ExponentialMovingAverage(double alpha) {
		this.alpha = alpha;
	}

	public ExponentialMovingAverage() {
		this(M1_ALPHA);
	}

	public void compute(Double value) {
		if (oldValue == null) {
			oldValue = value;
			currentValue = value;
		}
		double newValue = oldValue + alpha * (value - oldValue);
		oldValue = currentValue;
		currentValue = newValue;
	}

	public double getCurrentValue() {
		return currentValue;
	}

}
