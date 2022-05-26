package MyFirstKafkaProject.Demo2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "RqSEzrTI0iAIWmCPd1arYJZId";
    String consumerSecret = "7FEgQFV0UXua748OlcRuKHEcEEyJvrYojrwLgpcGKWdVUAbMRs";
    String token = "1505406563535708160-uvcVrr211QFsuo3aUiFP0oIhGk6dtF";
    String secret = "oNLjcpRnsxhaty9SwGuSXQuhLU6rsbMVdi9XErMLxsDOb";

    public TwitterProducer() {

    }

    public static void main(String[] args) {

        new TwitterProducer().run();
    }
    public void run() {

        String msg = null;

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        logger.info("Application started");

        // create twitter client
        Client client = createTwitterClient(msgQueue);
        // attempts to establish a connect
        client.connect();

        // create kafka producer

        // loop to send tweets to kafka
        while (!client.isDone()) {
           msg = null;
           try {
               msg = msgQueue.poll(5, TimeUnit.SECONDS);
           } catch (InterruptedException e) {
               e.printStackTrace();
               client.stop();
           }
           if (msg != null) {
               logger.info(msg);
           }
            logger.info("Application finished");
        }

    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Client hosebirdClient;

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        if (hosebirdAuth != null) {
            // make sure we been authorized by tweeter to connect
            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01") // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));

            hosebirdClient = builder.build();
        }
        else {
           hosebirdClient = null;
        }

        return hosebirdClient;

    }
}
