/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Stateful Enrichment" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */

public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// параметры
		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesInputPath = params.get("rides", pathToRideData);
		final String faresInputPath = params.get("fares", pathToFareData);

		// настройки задержки и скорости генерации
		final int maxDelaySeconds = 60;
		final int servingSpeed = 1800;

		// создание локального окружения, настройка сохранения состояния
		Configuration conf = new Configuration();
		conf.setString("state.backend", "filesystem");
		conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
		conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setParallelism(ExerciseBase.parallelism);
		// включение чекпоинтов каждые 10 секунд
		env.enableCheckpointing(10000L);
		env.getCheckpointConfig()
				.enableExternalizedCheckpoints(
						CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
				);

		// поток событий поездок, начало поездки
		DataStream<TaxiRide> rideStream = env
				.addSource(rideSourceOrTest(new TaxiRideSource(
						ridesInputPath, maxDelaySeconds, servingSpeed)))
				.filter(ride -> ride.isStart)
				.keyBy(ride -> ride.rideId);

		// поток событий трафиков
		DataStream<TaxiFare> fareStream = env
				.addSource(fareSourceOrTest(new TaxiFareSource(
						faresInputPath, maxDelaySeconds, servingSpeed)))
				.keyBy(fare -> fare.rideId);

		// соединение потоков по ключу rideId
		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedStream = rideStream
				.connect(fareStream)
				.flatMap(new JoinRidesAndFares())
				.uid("enrichment");

		// результат
		printOrTest(enrichedStream);
		env.execute("Объединение поездок с тарифами (java)");
	}

	// сохранение ожидающих поездов/тарифов и их выпуск при совпадении
	public static class JoinRidesAndFares extends RichCoFlatMapFunction<
			TaxiRide,
			TaxiFare,
			Tuple2<TaxiRide, TaxiFare>
			> {
		// состояние для незавершённой поездки
		private ValueState<TaxiRide> pendingRideState;
		// состояние для незавершённого тарифа
		private ValueState<TaxiFare> pendingFareState;

		@Override
		public void open(Configuration config) {
			// инициализация состояний
			pendingRideState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("pendingRide", TaxiRide.class)
			);
			pendingFareState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("pendingFare", TaxiFare.class)
			);
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// проверка, есть ли пришедший тариф
			TaxiFare savedFare = pendingFareState.value();
			if (savedFare != null) {
				// если тариф есть, очистка состояния, отправление пары
				pendingFareState.clear();
				out.collect(new Tuple2<>(ride, savedFare));
			} else {
				// иначе сохранение поездки и ожидание тарифа
				pendingRideState.update(ride);
			}
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide savedRide = pendingRideState.value();
			if (savedRide != null) {
				pendingRideState.clear();
				out.collect(new Tuple2<>(savedRide, fare));
			} else {
				pendingFareState.update(fare);
			}
		}
	}
}