package ee.ut.cs.dsg.windowingsemantics.timestampextractor;

import ee.ut.cs.dsg.windowingsemantics.events.SimpleEvent;
import ee.ut.cs.dsg.windowingsemantics.utilities.Utilities;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

// Extracts the embedded timestamp of a record (giving you "event time" semantics).
public class MyTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long l) {
        // `Foo` is your own custom class, which we assume has a method that returns
        // the embedded timestamp (in milliseconds).
        String myPojo = (String) record.value();


        if (myPojo != null) {
            String[] elems = myPojo.split(",");
            if (elems.length == 3 && Utilities.isNumeric(elems[0])) {
                return Long.parseLong(elems[0]);
            }
            else
                return System.currentTimeMillis();
        }


        else

    {
        // Kafka allows `null` as message value.  How to handle such message values
        // depends on your use case.  In this example, we decide to fallback to
        // wall-clock time (= processing-time).
        return System.currentTimeMillis();
    }
}


}