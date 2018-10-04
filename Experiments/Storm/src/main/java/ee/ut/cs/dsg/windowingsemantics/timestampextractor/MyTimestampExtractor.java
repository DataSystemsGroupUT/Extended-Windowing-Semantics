package ee.ut.cs.dsg.windowingsemantics.timestampextractor;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;

public class MyTimestampExtractor implements TimestampExtractor {
    public long extractTimestamp(Tuple tuple) {
        return Long.parseLong(tuple.getStringByField("Timestamp"));
    }
}
