package com.gunjan;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class PrcoessTweetFromKafkaAndWriteToInfluxDatabase
{
    public static void main1(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Long> someIntegers = env.generateSequence(0, 1000);
        
        IterativeStream<Long> iteration = someIntegers.iterate();
        
        DataStream<Long> minusOne = iteration.map(new MapFunction<Long,Long>()
        {
            @Override
            public Long map(Long value) throws Exception
            {
                return value - 1;
            }
        });
        
        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>()
        {
            @Override
            public boolean filter(Long value) throws Exception
            {
                return (value > 0);
            }
        });
        
        
        iteration.closeWith(stillGreaterThanZero);
        
        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>()
        {
            @Override
            public boolean filter(Long value) throws Exception
            {
                return (value <= 0);
            }
        });
        
        lessThanZero.print();
        
        env.execute("Twitter Streaming Example");
    }
    
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
        //env.enableCheckpointing(5000);
        
        DataStream<Tuple2<String,String>> controlKeyedStream = env.socketTextStream("10.71.69.60", 9999, "\n").map(controlEvent -> new Tuple2<String,String>("key", controlEvent))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String,String>>()
                {
                }))
                .keyBy(0);
        
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.71.69.236:31440");
        properties.setProperty("group.id", "flink");
        
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("admintome-test", new SimpleStringSchema(), properties);
        
        DataStream<Tuple2<String,String>> tweetKeyedStream = env.addSource(consumer).map(tweetStr -> new Tuple2<String,String>("key", tweetStr))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String,String>>()
                {
                }))
                .keyBy(0);
        
        ConnectedStreams<Tuple2<String,String>,Tuple2<String,String>> connectedStreams = tweetKeyedStream.connect(controlKeyedStream);
        
        final OutputTag<Tweet> outputTag = new OutputTag<Tweet>("side-output")
        {
        };
        
        SingleOutputStreamOperator<Tweet> tweetSingleOutputStreamOperator = connectedStreams.process(new KeyedCoProcessFunction<String,Tuple2<String,String>,Tuple2<String,String>,Tweet>()
        {
            private ValueState<Boolean> stopped = null;
            
            @Override
            public void open(Configuration config)
            {
                ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                        "have-stopped-key",
                        TypeInformation.of(new TypeHint<Boolean>()
                        {
                        }));
                stopped = getRuntimeContext().getState(descriptor);
                    descriptor.setQueryable("is-tweet-processing-stopped");
            }
            
            @Override
            public void processElement1(Tuple2<String,String> value, Context ctx, Collector<Tweet> out) throws Exception
            {
                try
                {
                    Tweet tweet = new MapToTweet().map(value.f1);
                    if(tweet.getText() != null && tweet.getText().length() > 0)
                    {
                        if(stopped.value() != null && !stopped.value().booleanValue())
                        {
                            out.collect(tweet);
                        }
                        else
                        {
                            ctx.output(outputTag, tweet);
                        }
                    }
                }
                catch(Exception e)
                {
                    //Ignore
                }
            }
            
            @Override
            public void processElement2(Tuple2<String,String> value, Context ctx, Collector<Tweet> out) throws Exception
            {
                if("true".equals(value.f1))
                {
                    stopped.update(Boolean.TRUE);
                }
                else
                {
                    stopped.update(Boolean.FALSE);
                }
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tweet>(Time.seconds(0))
        {
            
            @Override
            public long extractTimestamp(Tweet element)
            {
                return element.getTimestamp_ms();
            }
        });
        
        
        
        
        tweetSingleOutputStreamOperator.flatMap(new TokenizeTweetTextFlatMap())
                .keyBy((KeySelector<Tuple2<String,Integer>,Object>)hashtag -> hashtag.f0)
                .timeWindow(Time.seconds(300), Time.seconds(5)).sum(1)
                .timeWindowAll(Time.seconds(300), Time.seconds(5)).maxBy(1)
                .map(new MapToInfluxDBPoint()).addSink(new InfluxDBSink(influxDBConfig));
        
        tweetSingleOutputStreamOperator
                .map(tweet -> new Tuple1<>(1))
                .returns(TypeInformation.of(new TypeHint<Tuple1<Integer>>()
                {
                }))
                .windowAll(GlobalWindows.create())
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .sum(0)
                .map(new TotalTweetCountInfluxDBPoint())
                .addSink(new InfluxDBSink(influxDBConfig));
        
        tweetSingleOutputStreamOperator
                .map(tweet -> new Tuple2<>(1, tweet.getTimestamp_ms()))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>()
                {
                }))
                .timeWindowAll(Time.seconds(1))
                .sum(0)
                .map(new TweetPerSecondCountInfluxDBPoint())
                .addSink(new InfluxDBSink(influxDBConfig));
    
        DataStream<Tweet> unprocessedTweet = tweetSingleOutputStreamOperator.getSideOutput(outputTag);
        unprocessedTweet.print();
        
        unprocessedTweet
                .map(tweet -> new Tuple2<>(1, tweet.getTimestamp_ms()))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer,Long>>()
                {
                }))
                .timeWindowAll(Time.seconds(1))
                .sum(0)
                .map(new UnprocessedTweetPerSecondCountInfluxDBPoint())
                .addSink(new InfluxDBSink(influxDBConfig));
        
        env.execute("Twitter Streaming Example");
    }
    
    public static class TokenizeTweetTextFlatMap implements FlatMapFunction<Tweet,Tuple2<String,Integer>>
    {
        private static final long serialVersionUID = 1L;
        
        private transient ObjectMapper jsonParser;
        
        @Override
        public void flatMap(Tweet tweet, Collector<Tuple2<String,Integer>> out)
        {
            Pattern p = Pattern.compile("#\\w+");
            Matcher matcher = p.matcher(tweet.getText());
            while(matcher.find())
            {
                String cleanedHashtag = matcher.group(0).trim();
                if(cleanedHashtag != null)
                {
                    out.collect(new Tuple2<>(cleanedHashtag, 1));
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
                // That's ok, received a malformed document
            }
            return null;
        }
    }
    
    
    private static class MapToInfluxDBPoint extends RichMapFunction<Tuple2<String,Integer>,InfluxDBPoint>
    {
        @Override
        public InfluxDBPoint map(Tuple2<String,Integer> trendingHashTag) throws Exception
        {
            String table = "TrendingHashTag";
            long timestamp = System.currentTimeMillis();
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("hashtag", trendingHashTag.f0);
            fields.put("count", trendingHashTag.f1);
            return new InfluxDBPoint(table, timestamp, tags, fields);
        }
    }
    
    private static class TotalTweetCountInfluxDBPoint extends RichMapFunction<Tuple1<Integer>,InfluxDBPoint>
    {
        
        @Override
        public InfluxDBPoint map(Tuple1<Integer> totalTweetCount) throws Exception
        {
            String table = "TotalTweetCount";
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", totalTweetCount.f0);
            return new InfluxDBPoint(table, System.currentTimeMillis(), tags, fields);
        }
    }
    
    private static class TweetPerSecondCountInfluxDBPoint extends RichMapFunction<Tuple2<Integer,Long>,InfluxDBPoint>
    {
        
        @Override
        public InfluxDBPoint map(Tuple2<Integer,Long> tweetPerSecond) throws Exception
        {
            String table = "TweetPerSecondCount";
            HashMap<String,String> tags = new HashMap<>();
            HashMap<String,Object> fields = new HashMap<>();
            fields.put("count", tweetPerSecond.f0);
            return new InfluxDBPoint(table, tweetPerSecond.f1, tags, fields);
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
    
    private static class TweetAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tweet>
    {
        int count = 0;
        
        @Nullable
        @Override
        public Watermark getCurrentWatermark()
        {
            if(count > 30000) return new Watermark(Long.MAX_VALUE);
            return null;
        }
        
        @Override
        public long extractTimestamp(Tweet tweet, long l)
        {
            count++;
            return tweet.getTimestamp_ms();
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
}
