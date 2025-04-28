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

package com.ververica.flinktraining.exercises.datastream_java.process;

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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Java reference implementation for the "Expiring State" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */

public class ExpiringStateExercise extends ExerciseBase {
	// тег для несопоставленных поездок
	static final OutputTag<TaxiRide> unmatchedRideTag = new OutputTag<TaxiRide>("unmatchedRides") {};
	// тег для несопоставленных оплат
	static final OutputTag<TaxiFare> unmatchedFareTag = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {
		// чтение параметров запуска
		ParameterTool params = ParameterTool.fromArgs(args);
		String ridesFile = params.get("rides", ExerciseBase.pathToRideData); // файл с событиями поездок
		String faresFile = params.get("fares", ExerciseBase.pathToFareData); // файл с событиями оплат

		int maxDelay = 60; // максимальная задержка событий в секундах
		int speedupFactor = 600; // ускорение воспроизведения входного потока

		// Flink, настройка времени по событиям
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// фильтрация стартов с отбрасыванием каждого 1000 элемента
		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxDelay, speedupFactor)))
				.filter(r -> r.isStart && r.rideId % 1000 != 0)
				.keyBy(r -> r.rideId);

		// группировка по rideId
		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxDelay, speedupFactor)))
				.keyBy(f -> f.rideId);

		// соединение потоков и обработка в CoProcessFunction
		SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>> processed =
				rides.connect(fares)
						.process(new EnrichmentFunction());

		// вывод несопоставленных оплат в side-output
		printOrTest(processed.getSideOutput(unmatchedFareTag));

		env.execute("Expiring State (java)");
	}

	// соединение поездок и оплат по rideId, сбрасывание старых несопоставленных событий по таймеру
	public static class EnrichmentFunction
			extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		// состояние для хранения "ожидающей" поездки
		private ValueState<TaxiRide> rideValueState;
		// состояние для хранения "ожидающей" оплаты
		private ValueState<TaxiFare> fareValueState;

		@Override
		public void open(Configuration cfg) {
			// инициализация ValueState для поездок и оплат
			rideValueState = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("rideState", TaxiRide.class));
			fareValueState = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("fareState", TaxiFare.class));
		}

		@Override
		public void processElement1(
				TaxiRide ride,
				Context ctx,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiFare pendingFare = fareValueState.value();
			if (pendingFare != null) {
				// если оплата уже пришла, то ее очищение и вывод пары
				fareValueState.clear();
				ctx.timerService().deleteEventTimeTimer(pendingFare.getEventTime());
				out.collect(new Tuple2<>(ride, pendingFare));

			} else {
				// иначе сохранение поездки и таймер на время её события
				rideValueState.update(ride);
				ctx.timerService().registerEventTimeTimer(ride.getEventTime());
			}
		}

		@Override
		public void processElement2(
				TaxiFare fare,
				Context ctx,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide pendingRide = rideValueState.value();
			if (pendingRide != null) {
				rideValueState.clear();
				ctx.timerService().deleteEventTimeTimer(pendingRide.getEventTime());
				out.collect(new Tuple2<>(pendingRide, fare));

			} else {
				fareValueState.update(fare);
				ctx.timerService().registerEventTimeTimer(fare.getEventTime());
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			// по срабатыванию таймера вывод всех оставшихся несопоставленных событий
			TaxiFare leftoverFare = fareValueState.value();
			if (leftoverFare != null) {
				ctx.output(unmatchedFareTag, leftoverFare);
				fareValueState.clear();
			}
			TaxiRide leftoverRide = rideValueState.value();
			if (leftoverRide != null) {
				ctx.output(unmatchedRideTag, leftoverRide);
				rideValueState.clear();
			}
		}
	}
}