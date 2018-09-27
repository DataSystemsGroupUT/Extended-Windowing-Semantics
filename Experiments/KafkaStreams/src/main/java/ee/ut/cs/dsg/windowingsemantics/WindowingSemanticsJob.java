package ee.ut.cs.dsg.windowingsemantics;

import ee.ut.cs.dsg.windowingsemantics.events.SimpleEvent;
import ee.ut.cs.dsg.windowingsemantics.producer.MyKafkaProducer;
import ee.ut.cs.dsg.windowingsemantics.timestampextractor.MyTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KeyValue;


import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.KafkaStreams;

import java.util.Properties;


public class WindowingSemanticsJob {

    private static long slideWindowSize;
    private static long slideWindowSlide;
    private static long sessionWindowGap;
    private static long allowedLatness;

    public static void main(String[] args) {

        String fileName;
        //fileName= "Data.csv";
//        fileName = "DataIntWithDelay.csv";
//        fileName = "DummyDataWithDelay.csv";
        fileName = "DummyDataWithDelaySession.csv";
//        fileName = "DummyDataWithoutDelays.csv";
        slideWindowSize=10;
        slideWindowSlide=2;
        sessionWindowGap = 10;
        allowedLatness=10;
        streamingJob(WindowType.SlidingTime, true, fileName, 10);
//        streamingJob(WindowType.Session, true, fileName, 10);
    }
    private enum WindowType
    {
        SlidingTime,
        SlidingTuple,
        Session,
        Landmark
    }
    private static Object nullToString(Object o, String replacement)
    {
        if (o == null)
            return replacement;
        else
            return o;
    }
    private static void streamingJob(WindowType windowType, boolean generateData, String fileName, long allowedLateness)
    {
        boolean canContinue=true;
        if (generateData) {
            try {
                MyKafkaProducer.setInputFile(fileName);
                MyKafkaProducer.runProducer();
            }
            catch(Exception e)
            {
                canContinue = false;
                e.printStackTrace();

            }
        }
        if (canContinue) {
//        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
            Serde<String> stringSerde = Serdes.String();

            StreamsBuilder kStreamBuilder = new StreamsBuilder();
            KGroupedStream kGS =  kStreamBuilder.stream("measurement-topic", Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new MyTimestampExtractor()))
                    .map(new MyMapper2())
                    .groupByKey();

            if (windowType==WindowType.SlidingTime) {

                 kGS.windowedBy(TimeWindows.of(slideWindowSize).advanceBy(slideWindowSlide).until(allowedLateness))
                        .count()

                        .toStream(new KeyValueMapper<Windowed<String>, Long, String>() {

                            @Override
                            public String apply(Windowed<String> stringWindowed, Long aLong) {
                                String result = "(" + stringWindowed.window().start() + "," + stringWindowed.window().end() + "," + stringWindowed.key() + "," + aLong.toString() + ")";
                                System.out.println(result);
                                return result;
                            }
                        })
                        .map(new KeyValueMapper<String, Long, KeyValue<String, String>>() {
                            @Override
                            public KeyValue<String, String> apply(String s, Long aLong) {
                                return new KeyValue<>("",s);
                            }
                        })
                        .to("windowed-topic");
            }
            else if (windowType==WindowType.Session)
            {
                kGS.windowedBy(SessionWindows.with(sessionWindowGap).until(allowedLateness))
                        .count()

                        .toStream(new KeyValueMapper<Windowed<String>, Long, String>() {

                            @Override
                            public String apply(Windowed<String> stringWindowed, Long aLong) {
                                String result = "(" + stringWindowed.window().start() + "," + stringWindowed.window().end() + "," + stringWindowed.key() + "," + nullToString(aLong,"0").toString() + ")";
                                System.out.println(result);
                                return result;
                            }
                        })
                        .map(new KeyValueMapper<String, Long, KeyValue<String, String>>() {
                            @Override
                            public KeyValue<String, String> apply(String s, Long aLong) {
                                return new KeyValue<>("",s);
                            }
                        })
                        .to("windowed-topic");
            }


//    ByKey(stringSerde, "Counts")
//                .toStream()
//                .map((word, count) -> new KeyValue<>(word, word + ":" + count))
//                .to(stringSerde, stringSerde, "counts-topic");

            KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder.build(), getProperties());
            kafkaStreams.cleanUp();
            kafkaStreams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        }
    }
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "windowing-semantics-client");
        props.put("group.id", "test-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowing-semantics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    private static class MyMapper implements KeyValueMapper<String, String, KeyValue<String,SimpleEvent>>
    {

        @Override
        public KeyValue<String,SimpleEvent> apply(String s, String s2) {
            String[] elems = s2.split(",");
            SimpleEvent se = new SimpleEvent(Long.parseLong(elems[0]), Double.parseDouble(elems[2]), elems[1].trim());
            return new KeyValue<>(elems[1].trim(),se);
        }
    }
    private static class MyMapper2 implements KeyValueMapper<String, String, KeyValue<String,String>>
    {

        @Override
        public KeyValue<String,String> apply(String s, String s2) {
            String[] elems = s2.split(",");
//            SimpleEvent se = new SimpleEvent(Long.parseLong(elems[0]), Double.parseDouble(elems[2]), elems[1].trim());
            return new KeyValue<>(elems[1].trim(),"1");
        }
    }

}
