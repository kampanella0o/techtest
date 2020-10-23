package com.maksym.techtest.util;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;

public class SubscriptionCreater {

    public static void createPullSubscriptionExample() throws IOException {

        String projectId = "extreme-water-293016";
        String subscriptionId = "techtest-bucket-test-subscription";
        String topicId = "techtest-bucket-test";

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            ProjectSubscriptionName subscriptionName =
                    ProjectSubscriptionName.of(projectId, subscriptionId);
            Subscription subscription =
                    subscriptionAdminClient.createSubscription(
                            subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.println("Created pull subscription: " + subscription.getName());
        }
    }

    public static void main(String[] args) throws IOException {
        SubscriptionCreater.createPullSubscriptionExample();
    }


}
