package MyFirstKafkaProject.Demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String boostrapServers = "127.0.0.1:9092";

        // create Producer properties
        // check site for all properties
        // https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            // create the producer

            String topic = "first_topic";
            String value = "Hello world";
            String key = "id_" + Integer.toString(i);

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

            logger.info("Key: " + key);  // log the key
            // id_0 going to partition 1
            // id_1 going to partition 0
            // id_2 going to partition 2
            // id_3 going to partition 0
            // id_4 going to partition 1
            // id_5 going to partition 2
            // id_6 going to partition 1
            // id_7 going to partition 0
            // id_8 going to partition 2
            // id_9 going to partition 0

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() );
                    }
                    else {
                        logger.error("Error while producing", e);
                    }
            }
        }).get(); // block the .send() to make it synchronous - don't do this in production!

        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }

}
