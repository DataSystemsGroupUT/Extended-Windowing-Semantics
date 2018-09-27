package ee.ut.cs.dsg.windowingsemantics.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MyKafkaProducer {
    private final static String TOPIC = "measurement-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String fileName;

    public static void setInputFile(String inputFileName)
    {
        fileName = inputFileName;
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer() throws Exception {
        final Producer<String, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            line = reader.readLine();
            while (line != null)
            {
                if (line.startsWith("*"))
                {
                    line = reader.readLine();
                    continue;
                }

                final ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, "Dummy",line);
                RecordMetadata metadata = producer.send(record).get();

                line = reader.readLine();
            }
            reader.close();
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
        finally {
            producer.flush();
            producer.close();
        }
    }
}
