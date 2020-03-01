/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package PIPELINE;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import Producer.SolaceProducer;
import Sink.MysqlSinkAPP;
import Sink.MysqlSinkODS;
import Utils.windowFuncDiff;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	public static JsonObject stringToJson(final String x) {

		JsonObject res = JsonParser.parseString(x).getAsJsonObject();
		return res;
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program

		DataStream<JsonObject> ods = env.addSource(new SolaceProducer()).map(x -> stringToJson(x));
		ods.addSink(new MysqlSinkODS());
		int windowTime = 10;
		DataStream<Tuple4<Float, Float, Float, String>> main_stream = ods.map(x -> {
			return new Tuple4<Float, Float, Float, String>(
				Float.valueOf(x.get("Pressure").getAsString()),
				Float.valueOf(x.get("Humidity").getAsString()),
				Float.valueOf(x.get("Temperature").getAsString()),
				x.get("Device_Name").getAsString()
				);
		}).returns(Types.TUPLE(Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.STRING));;
		DataStream<JsonObject> pressure = main_stream.keyBy(3).window(TumblingProcessingTimeWindows.of(Time.seconds(windowTime))).apply(new windowFuncDiff("0", "Pressure")).map(x -> stringToJson(x));
		DataStream<JsonObject> humidity = main_stream.keyBy(3).window(TumblingProcessingTimeWindows.of(Time.seconds(windowTime))).apply(new windowFuncDiff("1", "Humidity")).map(x -> stringToJson(x));
		DataStream<JsonObject> temperature = main_stream.keyBy(3).window(TumblingProcessingTimeWindows.of(Time.seconds(windowTime))).apply(new windowFuncDiff("2", "Temperature")).map(x -> stringToJson(x));
		pressure.addSink(new MysqlSinkAPP());
		humidity.addSink(new MysqlSinkAPP());
		temperature.addSink(new MysqlSinkAPP());
		
		pressure.print();
		humidity.print();
		temperature.print();
		
		ods.print();
		
		env.execute("Flink Streaming Java API Skeleton");
	}
}
