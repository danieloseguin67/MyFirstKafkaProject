package MyFirstKafkaProject.Demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.lang.Runtime.getRuntime;
import static org.omg.SendingContext.RunTime.*;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {

    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String boostrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        // latch for dealing with multiple threads
        logger.info("Creating the consumer runnable thread");

        // create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(
                boostrapServers,
                groupId,
                topic,
                latch);

        // start th thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
             latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch ) {
           this.latch = latch;

           Properties properties = new Properties();
           properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
           properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
           properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
           properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId );
           // possible values ::= earliest, latest, none
           properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

           // create consumer
           logger.info("Creating consumer");
           KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

           // subscribe consumer
           logger.info("consumer is subscribing");
           consumer.subscribe(Arrays.asList(topic)); // we can add as many topics as desired
           // alternatively we could do the following for one topic
           //consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {

            // poll for new data
            try {

               while (true) {
                  ConsumerRecords<String, String> records =
                          consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0.
                  for (ConsumerRecord record : records) {
                      logger.info("Key:" + record.key() + ", Value: " + record.value());
                      logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                  }
               }
            } catch (WakeupException e) {
                 logger.info("Received shutdown signal!");
            } finally {
                 consumer.close();
                 // tell our main code we're done with the consumer
                 latch.countDown();
            }
        }

        public void shutdown() {
           // the wakeup() method is a special method to interrupt consumer.poll()
           // it will throw the exception WakeUpException
           consumer.wakeup();
        }
    }
}
