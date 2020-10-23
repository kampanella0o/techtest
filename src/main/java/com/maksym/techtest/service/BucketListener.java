package com.maksym.techtest.service;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.StorageException;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Service
public class BucketListener {
    private Logger logger = LoggerFactory.getLogger(AvroParser.class);


    private final
    AvroParser avroParser;

    public BucketListener(AvroParser avroParser) {
        this.avroParser = avroParser;
    }

    @PostConstruct
    public void checkMessagesFromBucket() {
        String projectId = System.getenv("ProjectID");
        String subscriptionId = System.getenv("SubscriptionID");

        logger.info("Creating PubSub subscription on project {} with id {}", projectId, subscriptionId);
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);

        logger.info("Instantiating an asynchronous message receiver");
        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    logger.info("Handling incoming message");
                    String fileName = message.getAttributesOrThrow("objectId");
                    String eventType = message.getAttributesOrThrow("eventType");
                    logger.info("Event type: {}, file name: {}", eventType, fileName);
                    consumer.ack();
                    if (eventType.equals("OBJECT_FINALIZE")){
                        if (FilenameUtils.getExtension(fileName).equals("avro")){
                            try {
                                avroParser.parseAvroFile(FilenameUtils.getName(fileName));
                            } catch (StorageException e) {
                                logger.error("Failed to connect to Google Cloud Storage: ", e);
                            } catch (IOException e) {
                                logger.error("Failed to process the file: ", e);
                            } catch (RuntimeException e) {
                                logger.error("Something went wrong during parsing the file: ", e);
                            }
                        }
                    }
                };

        logger.info("Instantiating the subscriber");
        ExecutorProvider executorProvider =
                InstantiatingExecutorProvider.newBuilder()
                        .setExecutorThreadCount(1)
                        .build();
        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver).setExecutorProvider(executorProvider).build();
        subscriber.startAsync().awaitRunning();
        logger.info("Listening for messages on {}: ", subscriptionName.toString());
    }

}
