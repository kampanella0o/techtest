package com.maksym.techtest.service;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class BucketListener {
    private Logger logger = LoggerFactory.getLogger(AvroParser.class);


    @Autowired
    AvroParser avroParser;

    @PostConstruct
    public void checkMessagesFromBucket() {
        String projectId = "extreme-water-293016";
        String subscriptionId = "myermolenko-new-bucket-subscription";

        logger.info("Creating PubSub subscription on project {} with id {}", projectId, subscriptionId);
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);

        logger.info("Instantiating an asynchronous message receiver");
        // Instantiate an asynchronous message receiver.
        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    // Handle incoming message, then ack the received message.
                    logger.info("Handling incoming message");
                    String fileName = message.getAttributesOrThrow("objectId");
                    logger.info("Event type: {}, file name: {}", message.getAttributesOrThrow("eventType"), fileName);
                    if (message.getAttributesOrThrow("eventType").equals("OBJECT_FINALIZE")){
                        if (FilenameUtils.getExtension(message.getAttributesOrThrow("objectId")).equals("avro")){
                            avroParser.parseAvroFile(fileName);
                        }
                    }
                    consumer.ack();
                };

        logger.info("Instantiating the subscriber");
        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            // Start the subscriber.
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
//            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (Exception timeoutException) {
//        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber after 30s. Stop receiving messages.
            //TODO should I shut down the subscriber anyhow?
//            subscriber.stopAsync();
        }
    }

}
