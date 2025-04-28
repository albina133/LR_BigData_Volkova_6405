/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */

public class HourlyTipsExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// параметры из аргументов
		ParameterTool params = ParameterTool.fromArgs(args);
		final String fareInputPath = params.get("input", ExerciseBase.pathToFareData);

		// максимальная задержка событий и коэффициент ускорения генерации
		final int maxOutOfOrderSeconds = 60;
		final int speedupFactor = 600;

		// окружение flink, настройка времени событий и параллелизм
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// получение потока чаевых из источника
		DataStream<TaxiFare> fareStream = env.addSource(
				fareSourceOrTest(new TaxiFareSource(fareInputPath, maxOutOfOrderSeconds, speedupFactor))
		);

		// суммирование чаевых по каждому водителю за час
		DataStream<Tuple3<Long, Long, Float>> hourlyMaxTips = fareStream
				.keyBy(fare -> fare.driverId) // группировка по идентификатору водителя
				.timeWindow(Time.hours(1)) // окно в 1 час
				.process(new SumTipsPerDriver()) // суммирование чаевые
				.timeWindowAll(Time.hours(1)) // глобальное окно в 1 час
				.maxBy(2); // максимальная сумма чаевых

		// вывод результата
		printOrTest(hourlyMaxTips);

		// запускаем выполнение конвейера
		env.execute("Hourly Tips (java)");
	}

	// окно для суммирования чаевых по водителям
	public static class SumTipsPerDriver extends ProcessWindowFunction<
			TaxiFare, // записи с информацией о чаевых
			Tuple3<Long, Long, Float>, // конец окна, id водителя, сумма чаевых
			Long, // id водителя
			TimeWindow // временное окно
			> {
		@Override
		public void process(
				Long driverId, // идентификатор водителя
				Context context, // контекст окна
				Iterable<TaxiFare> faresInWindow, // все записи чаевых в окне
				Collector<Tuple3<Long, Long, Float>> out // коллектор для результата
		) throws Exception {
			// суммирование чаевых
			float totalTips = 0f;
			for (TaxiFare fare : faresInWindow) {
				totalTips += fare.tip;
			}
			// получение времени окончания окна
			long windowEnd = context.window().getEnd();
			// вывод конца окна, id водителя, суммы чаевых
			out.collect(new Tuple3<>(windowEnd, driverId, totalTips));
		}
	}
}