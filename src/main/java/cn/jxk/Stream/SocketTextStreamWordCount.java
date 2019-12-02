package cn.jxk.Stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        String hostname = "127.0.0.1";
        int port = 9000;
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        //计数
      /*  SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);*/
        KeyedStream<Tuple2<Integer, String>, Tuple> tuple2TupleKeyedStream = stream.map((n) -> {
            int i = new Random().nextInt(4);
            return Tuple2.of(i, n);
        }).returns(Types.TUPLE(Types.INT, Types.STRING)).setParallelism(3).keyBy(0);
        tuple2TupleKeyedStream.print();
        DataStream dataStreamSource = tuple2TupleKeyedStream.timeWindow(Time.seconds(60)).trigger(
                new CountTriggerWithTimeout(4, TimeCharacteristic.ProcessingTime)
        ).apply(new WindowFunction<Tuple2<Integer, String>, List<String>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple integer, TimeWindow window, Iterable<Tuple2<Integer, String>> input, Collector<List<String>> out) throws Exception {
                List<String> mutations = new ArrayList<String>();
                for (Tuple2<Integer, String> mess : input) {
                    mutations.add(mess.f1);
                }
                out.collect(mutations);
            }
        });

        dataStreamSource.print();
        env.execute("Java WordCount from SocketTextStream Example");
//        System.out.println(env.getExecutionPlan());
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
