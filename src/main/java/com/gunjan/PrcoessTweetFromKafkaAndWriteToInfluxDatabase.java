package com.gunjan;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class PrcoessTweetFromKafkaAndWriteToInfluxDatabase
{
    public static void main(String[] args) throws Exception
    {
            InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("http://10.71.69.236:31948", "root", "root", "twittergraph")
                .batchActions(-1)
                .flushDuration(100, TimeUnit.MILLISECONDS)
                .enableGzip(true)
                .build();
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStateBackend(new FsStateBackend("file:///data/flink/checkpoints"));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.71.69.236:31117");
        properties.setProperty("group.id", "flink");
        
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("admintome-test", new SimpleStringSchema(), properties);
        
        SingleOutputStreamOperator<Tweet> tweetSingleOutputStreamOperator = env
                .addSource(consumer)
                .map(new MapToTweet())
                .assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());
        
        ProcessWindowFunction<Tuple2<String,Long>,Tuple3<String,Long,Timestamp>,String,TimeWindow> processFucntion =
                new ProcessWindowFunction<Tuple2<String,Long>,Tuple3<String,Long,Timestamp>,String,TimeWindow>()
                {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String,Long>> elements, Collector<Tuple3<String,Long,Timestamp>> out) throws Exception
                    {
                        elements.forEach(stringIntegerLongTuple3 -> out.collect(new Tuple3<>(stringIntegerLongTuple3.f0, stringIntegerLongTuple3.f1, new Timestamp(context.window().getEnd()))));
                    }
                };
        tweetSingleOutputStreamOperator.flatMap(new TokenizeTweetTextFlatMap())
                .keyBy((KeySelector<Tuple2<String,Long>,String>)stringIntegerLongTuple3 -> stringIntegerLongTuple3.f0)
                .timeWindow(Time.seconds(30), Time.seconds(5))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .aggregate(new CustomSumAggregator(), processFucntion)
                .timeWindowAll(Time.seconds(30), Time.seconds(5))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .maxBy(1)
                .map(new MapToTrendingHashTagInfluxDBPoint())
                .addSink(new InfluxDBSink(influxDBConfig));
        
        tweetSingleOutputStreamOperator
                .map(tweet -> 1L)
                .returns(TypeInformation.of(new TypeHint<Long>()
                {
                }))
                .windowAll(GlobalWindows.create())
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .sum(0)
                .map(new TotalTweetCountInfluxDBPoint())
                .addSink(new InfluxDBSink(influxDBConfig));
        
        tweetSingleOutputStreamOperator
                .timeWindowAll(Time.seconds(1))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .apply((AllWindowFunction<Tweet,Tuple2<Timestamp,Long>,TimeWindow>)(window, values, out) -> {
                    long count = 0;
                    Iterator<Tweet> iterator = values.iterator();
                    while(iterator.hasNext())
                    {
                        iterator.next();
                        count++;
                    }
                    
                    out.collect(new Tuple2<>(new Timestamp(window.getEnd()), count));
                }, TypeInformation.of(new TypeHint<Tuple2<Timestamp,Long>>()
                {
                })).map(new TweetPerSecondCountInfluxDBPoint())
                .addSink(new InfluxDBSink(influxDBConfig));
        
        env.execute("Twitter Streaming Example");
    }
    
    public static class TokenizeTweetTextFlatMap implements FlatMapFunction<Tweet,Tuple2<String,Long>>
    {
        private static final long serialVersionUID = 1L;
        
        private transient ObjectMapper jsonParser;
        
        @Override
        public void flatMap(Tweet tweet, Collector<Tuple2<String,Long>> out)
        {
            Pattern p = Pattern.compile("#\\w+");
            Matcher matcher = p.matcher(tweet.getText());
            while(matcher.find())
            {
                String cleanedHashtag = matcher.group(0).trim();
                if(cleanedHashtag != null)
                {
                    out.collect(new Tuple2<>(cleanedHashtag, 1L));
                }
            }
        }
    }
    
    private static class MapToTweet implements MapFunction<String,Tweet>
    {
        
        @Override
        public Tweet map(String s) throws Exception
        {
            ObjectMapper mapper = new ObjectMapper();
            try
            {
                return mapper.readValue(s, Tweet.class);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            return null;
        }
    }
    
    
    private static class MapToTrendingHashTagInfluxDBPoint extends RichMapFunction<Tuple3<String,Long,Timestamp>,InfluxDBPoint>
    {
        @Override
        public InfluxDBPoint map(Tuple3<String,Long,Timestamp> trendingHashTag) throws Exception
        {
            String table = "TrendingHashTag";
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("hashtag", trendingHashTag.f0);
            fields.put("count", trendingHashTag.f1);
            return new InfluxDBPoint(table, trendingHashTag.f2.getTime(), tags, fields);
        }
    }
    
    private static class TotalTweetCountInfluxDBPoint implements MapFunction<Long,InfluxDBPoint>
    {
        
        @Override
        public InfluxDBPoint map(Long count) throws Exception
        {
            String table = "TotalTweetCount";
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", count);
            // Below System.currentTimeMillis() is wrong ... need to pass trigger time of global window
            return new InfluxDBPoint(table, System.currentTimeMillis(), tags, fields);
        }
    }
    
    private static class TweetPerSecondCountInfluxDBPoint extends RichMapFunction<Tuple2<Timestamp,Long>,InfluxDBPoint>
    {
        
        @Override
        public InfluxDBPoint map(Tuple2<Timestamp,Long> tweetPerSecond) throws Exception
        {
            String table = "TweetPerSecondCount";
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", tweetPerSecond.f1);
            return new InfluxDBPoint(table, tweetPerSecond.f0.getTime(), tags, fields);
        }
    }
    
    private static class UnprocessedTweetPerSecondCountInfluxDBPoint extends RichMapFunction<Tuple2<Integer,Long>,InfluxDBPoint>
    {
        
        @Override
        public InfluxDBPoint map(Tuple2<Integer,Long> tweetPerSecond) throws Exception
        {
            String table = "StoppedTweetPerSecondCount";
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", tweetPerSecond.f0);
            return new InfluxDBPoint(table, tweetPerSecond.f1, tags, fields);
        }
    }
    
    public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tweet>
    {
        
        private final long maxTimeLag = 300000;
        
        @Override
        public long extractTimestamp(Tweet element, long previousElementTimestamp)
        {
            return element.getTimestamp_ms();
        }
        
        @Override
        public Watermark getCurrentWatermark()
        {
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
    
    private static class SumAggregate implements AggregateFunction<Long,Long,Long>
    {
        
        @Override
        public Long createAccumulator()
        {
            return new Long(0);
        }
        
        @Override
        public Long add(Long aLong, Long aLong2)
        {
            return aLong + aLong2;
        }
        
        @Override
        public Long getResult(Long aLong)
        {
            return aLong;
        }
        
        @Override
        public Long merge(Long aLong, Long acc1)
        {
            return aLong + acc1;
        }
    }
    
    private static class TrendingHashTag implements AllWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,TimeWindow>
    {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<String,Integer>> values, Collector<Tuple2<String,Integer>> out) throws Exception
        {
            Tuple2<String,Integer> max = new Tuple2<>("", 0);
            Iterator<Tuple2<String,Integer>> iterator = values.iterator();
            while(iterator.hasNext())
            {
                Tuple2<String,Integer> current = iterator.next();
                if(max.f1 < current.f1)
                {
                    max = current;
                }
            }
            out.collect(max);
        }
    }
    
    public static class CustomSumAggregator implements AggregateFunction<Tuple2<String,Long>,Tuple2<String,Long>,Tuple2<String,Long>>
    {
        @Override
        public Tuple2<String,Long> createAccumulator()
        {
            return new Tuple2<String,Long>("", 0L);
        }
        
        @Override
        public Tuple2<String,Long> add(Tuple2<String,Long> stringIntegerLongTuple3, Tuple2<String,Long> stringIntegerLongTuple32)
        {
            return new Tuple2<>(stringIntegerLongTuple3.f0, stringIntegerLongTuple3.f1 + stringIntegerLongTuple32.f1);
        }
        
        @Override
        public Tuple2<String,Long> getResult(Tuple2<String,Long> stringIntegerLongTuple3)
        {
            return stringIntegerLongTuple3;
        }
        
        @Override
        public Tuple2<String,Long> merge(Tuple2<String,Long> stringIntegerLongTuple3, Tuple2<String,Long> acc1)
        {
            return new Tuple2<>(stringIntegerLongTuple3.f0, stringIntegerLongTuple3.f1 + acc1.f1);
        }
    }
}
