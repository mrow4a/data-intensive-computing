package org.apache.flink.dataintensive;

/**
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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import scala.collection.Iterable;

import java.util.Iterator;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class PopularPlaces {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");

        final int maxEventDelay = 60;                // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600;          // events of 10 minutes are served in 1 second
        final int popularPlacesThreshold = 20;        // threshold for popular places

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        SingleOutputStreamOperator<Tuple5<Float, Float, Long, Boolean, Integer>> filteredRides =
                rides
                // filter out rides that do not start or stop in NYC
                .filter(new FilterFunction<TaxiRide>() {
                    @Override
                    public boolean filter (TaxiRide taxiRide) throws Exception {

                        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                                GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
                    }
                })
                .map(new MapFunction<TaxiRide, Tuple2<Integer, Boolean>>() {
                    @Override
                    public Tuple2<Integer, Boolean> map(TaxiRide value) throws Exception {
                        return new Tuple2<>(
                                GeoUtils.mapToGridCell(value.startLon, value.startLat),
                                value.isStart
                        );
                    }
                })
                .keyBy(0, 1)
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new WindowFunction<Tuple2<Integer, Boolean>, Tuple4<Integer, Boolean, Long, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, java.lang.Iterable<Tuple2<Integer, Boolean>> input, Collector<Tuple4<Integer, Boolean, Long, Integer>> out) throws Exception {
                        Integer cellId = tuple.getField(0);
                        Boolean isStart = tuple.getField(1);
                        Long windowEnd = window.getEnd();

                        int count = 0;
                        for (Iterator i = input.iterator(); i.hasNext(); i.next())
                            count++;

                        out.collect(new Tuple4<>(cellId, isStart, windowEnd, count));
                    }
                })
                .filter(new FilterFunction<Tuple4<Integer, Boolean, Long, Integer>>() {

                    @Override
                    public boolean filter(Tuple4<Integer, Boolean, Long, Integer> value) throws Exception {
                        return (value.f3 > popularPlacesThreshold);
                    }
                })
                .map(new MapFunction<Tuple4<Integer,Boolean,Long,Integer>, Tuple5<Float, Float, Long, Boolean, Integer>>() {

                    @Override
                    public Tuple5<Float, Float, Long, Boolean, Integer> map(Tuple4<Integer, Boolean, Long, Integer> value) throws Exception {
                        return new Tuple5<>(
                                GeoUtils.getGridCellCenterLon(value.f0), // CellCenterLon
                                GeoUtils.getGridCellCenterLat(value.f0), // CellCenterLat
                                value.f2,                                // WindowTimestamp
                                value.f1,                                // Departure/Arrival - true/false
                                value.f3                                 // Count of arrivals/departures
                        );
                    }
                });

        // print the filtered stream
        filteredRides.print();

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleaning");
    }
}
