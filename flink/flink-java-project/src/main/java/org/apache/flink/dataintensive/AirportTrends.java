package org.apache.flink.dataintensive;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class AirportTrends {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid){
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid){
            for(JFKTerminal terminal : values()){
                if(terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }

        public static int getHour(long timestamp) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            calendar.setTimeInMillis(timestamp);
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
    }

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");

        final int maxEventDelay = 60;                // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600;          // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // We will use here event time.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        // Construct pipeline to get top terminals each hour
        SingleOutputStreamOperator<Tuple3<JFKTerminal, Integer, Integer>> filteredRides =
                // Start with the raw rides
                rides
                //
                // Map to grid cell and determine which terminal it is
                //
                .map(new MapFunction<TaxiRide, Tuple2<JFKTerminal, DateTime>>() {
                    @Override
                    public Tuple2<JFKTerminal, DateTime> map(TaxiRide value) throws Exception {
                        if (value.isStart) {
                            return new Tuple2<>(
                                    JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(value.startLon, value.startLat)),
                                    value.startTime
                            );
                        } else {
                            return new Tuple2<>(
                                    JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(value.endLon, value.endLat)),
                                    value.endTime
                            );
                        }
                    }
                })
                //
                // Filter out the ones which are not in area of the airport
                //
                .filter(new FilterFunction<Tuple2<JFKTerminal, DateTime>>() {
                    @Override
                    public boolean filter(Tuple2<JFKTerminal, DateTime> taxiRide) throws Exception {
                        return (taxiRide.f0 != JFKTerminal.NOT_A_TERMINAL);
                    }
                })
                //
                // We dont need to assign watermarks, since TaxiRideSource does it internally
                //
                // Calculate what is an hour of that apperiance at airport
                // This is calculated after filtering to reduce number of computations
                //
                .map(new MapFunction<Tuple2<JFKTerminal, DateTime>, Tuple2<JFKTerminal, Integer>>() {
                    @Override
                    public Tuple2<JFKTerminal, Integer> map(Tuple2<JFKTerminal, DateTime> value) throws Exception {
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                        calendar.setTimeInMillis(value.f1.getMillis());
                        return new Tuple2<>(value.f0, calendar.get(Calendar.HOUR_OF_DAY));
                    }
                })
                //
                // Key by terminal
                //
                .keyBy(0)
                //
                // Use TumblingEventTimeWindows to group events by fixed
                // time windows of 1h e.g. 1:00-1:59, 2:00-2:59
                // Watermarks are already assigned in TaxiRideSource
                //
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //
                // Apply count on the specific hour window
                //
                .apply(new WindowFunction<Tuple2<JFKTerminal, Integer>, Tuple3<JFKTerminal, Integer, Integer>, Tuple, TimeWindow>() {
                    /**
                     * Output here will be as follows
                     *
                     * 3> (TERMINAL_4,30,19)
                     * 4> (TERMINAL_6,1,19)
                     * 1> (TERMINAL_3,26,19)
                     * 1> (TERMINAL_3,39,20)
                     * 3> (TERMINAL_4,7,20)
                     * 3> (TERMINAL_4,6,21)
                     * 4> (TERMINAL_6,1,22)
                     * 1> (TERMINAL_3,16,21)
                     * 1> (TERMINAL_3,3,22)
                     * 1> (TERMINAL_3,10,23)
                     * 2> (TERMINAL_2,2,23)
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<JFKTerminal, Integer>> input, Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception {
                        JFKTerminal terminal = tuple.getField(0);

                        // Gets the starting timestamp of the window and converts to the miliseconds since epoch.
                        // This is the first timestamp that belongs to this window.
                        int recordingHour = JFKTerminal.getHour(window.getStart());

                        // Count visits at that hour
                        int count = 0;
                        for (Iterator i = input.iterator(); i.hasNext(); i.next()) {
                            count++;
                        }

                        // Send tuple with termina, count and hour at which data has been collected
                        out.collect(new Tuple3<>(terminal, count, recordingHour));
                    }
                })
                //
                // Watermarks are already assigned in TaxiRideSource, so we dont need to assign
                //
//                // Assign timestamps and watermarks
//                // Our assigned timestamp will be just the timestamp of the taxi ride (arrival/departure)
//                // We will also use AssignerWithPeriodicWatermarks since stream might be unordered
//                // Use in that case OutOfOrderness of 1 minute
//                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<JFKTerminal, DateTime>>() {
//
//                    private final long maxOutOfOrderness = 60000; // 1 minute
//
//                    private long currentMaxTimestamp;
//
//                    @Override
//                    public Watermark getCurrentWatermark() {
//                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
//                    }
//
//                    @Override
//                    public long extractTimestamp(Tuple2<JFKTerminal, DateTime> element, long previousElementTimestamp) {
//                        long timestamp = element.f1.getMillis();
//                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
//                        return timestamp;
//                    }
//                })
                //
                // Now, as we have distinct terminals and their counts for specific hours, window them all within
                // the same hours and select top used
                //
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new AllWindowFunction<Tuple3<JFKTerminal, Integer, Integer>, Tuple3<JFKTerminal, Integer, Integer>, TimeWindow>() {
                    /*
                     * Output here will be as follows
                     *
                     * 1> (TERMINAL_4,30,19)
                     * 2> (TERMINAL_3,39,20)
                     * 3> (TERMINAL_3,16,21)
                     * 4> (TERMINAL_3,3,22)
                     * 1> (TERMINAL_4,14,23)
                     * 2> (TERMINAL_4,45,0)
                     * 3> (TERMINAL_4,70,1)
                     * 4> (TERMINAL_3,48,2)
                     * 1> (TERMINAL_4,42,3)
                     * 2> (TERMINAL_4,45,4)
                     * 3> (TERMINAL_4,31,5)
                     */

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<JFKTerminal, Integer, Integer>> values, Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception {
                                                // Gets the starting timestamp of the window and converts to the miliseconds since epoch.
                        // This is the first timestamp that belongs to this window.
                        int recordingHour = JFKTerminal.getHour(window.getStart());

                        // Count top visits at that hour
                        int highestCount = 0;
                        JFKTerminal highestTerminal = JFKTerminal.NOT_A_TERMINAL;
                        for(Tuple3<JFKTerminal, Integer, Integer> value : values) {
                            if (highestCount < value.f1) {
                                highestCount = value.f1;
                                highestTerminal = value.f0;
                            }
                        }

                        out.collect(new Tuple3<>(highestTerminal, highestCount, recordingHour));
                    }
                });

        // print the filtered stream
        filteredRides.print();

        env.execute();

    }
}