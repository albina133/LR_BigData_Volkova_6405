/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.ververica.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */

public class RideCleansingExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// чтение параметров
		ParameterTool params = ParameterTool.fromArgs(args);
		String ridesInputPath = params.get("input", ExerciseBase.pathToRideData); // путь к файлу с данными о поездках

		int maxDelaySec = 60; // максимальная задержка событий в секундах
		int speedFactor = 600; // ускорение воспроизведения потока

		// Flink
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

		// источник потока событий TaxiRide
		DataStream<TaxiRide> rideStream = env.addSource(
				rideSourceOrTest(new TaxiRideSource(ridesInputPath, maxDelaySec, speedFactor))
		);

		// фильтрация поездок в пределах Нью-Йорка
		DataStream<TaxiRide> cleansedRides = rideStream.filter(new RideCleansingFilter());

		// вывод очищенного потока
		printOrTest(cleansedRides);

		// запуск приложения
		env.execute("Taxi Ride Cleansing");
	}

	// фильтр, который пропускает только поездки, начинающиеся и заканчивающиеся в зоне NYC
	public static class RideCleansingFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide ride) {
			// true, если и старт и финиш находятся в NYC
			return GeoUtils.isInNYC(ride.startLon, ride.startLat)
					&& GeoUtils.isInNYC(ride.endLon, ride.endLat);
		}
	}
}