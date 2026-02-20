package com.example;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.PartitionProperties;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;

import java.time.Duration;
import java.util.List;

public class FunctionApp {

    private static DefaultAzureCredential credential() {
        // Uses User Assigned Managed Identity when AZURE_CLIENT_ID is set in Function App settings
        return new DefaultAzureCredentialBuilder().build();
    }

    /**
     * Timer-trigger function:
     * Lists consumer groups and partitions for a given Event Hub on a schedule.
     *
     * Schedule examples:
     * - "0 */5 * * * *" = every 5 minutes
     * - "0 0 * * * *"   = every hour
     */
    @FunctionName("DescribeEventHubOnSchedule")
    public void describeEventHubOnSchedule(
            @TimerTrigger(
                    name = "timer",
                    schedule = "0 */5 * * * *"   // every 5 minutes
            )
            String timerInfo,
            final ExecutionContext context) {

        String fqns = System.getenv("EVENTHUB_FQNS");     // <namespace>.servicebus.windows.net
        String eventHub = System.getenv("EVENTHUB_NAME"); // <eventhub-name>

        if (fqns == null || fqns.isBlank() || eventHub == null || eventHub.isBlank()) {
            context.getLogger().severe("Missing EVENTHUB_FQNS and/or EVENTHUB_NAME app settings.");
            return;
        }

        EventHubConsumerAsyncClient consumer = null;
        try {
            consumer = new EventHubClientBuilder()
                    .fullyQualifiedNamespace(fqns)
                    .eventHubName(eventHub)
                    .credential(credential())
                    .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                    .buildAsyncConsumerClient();

            // Consumer groups (data-plane; requires appropriate RBAC on the Event Hubs namespace/event hub)
            List<String> consumerGroups = consumer.listConsumerGroups()
                    .collectList()
                    .block(Duration.ofSeconds(20));

            context.getLogger().info("Namespace: " + fqns + " | EventHub: " + eventHub);
            if (consumerGroups == null || consumerGroups.isEmpty()) {
                context.getLogger().info("Consumer groups: (none returned)");
            } else {
                context.getLogger().info("Consumer groups: " + consumerGroups);
            }

            // Partitions + properties
            List<String> partitions = consumer.getPartitionIds()
                    .collectList()
                    .block(Duration.ofSeconds(20));

            if (partitions == null || partitions.isEmpty()) {
                context.getLogger().info("Partitions: (none returned)");
            } else {
                context.getLogger().info("Partitions: " + partitions);
                for (String pid : partitions) {
                    PartitionProperties props = consumer.getPartitionProperties(pid)
                            .block(Duration.ofSeconds(20));

                    if (props == null) {
                        context.getLogger().info("Partition " + pid + ": (no properties)");
                        continue;
                    }

                    context.getLogger().info(
                            "Partition " + pid
                                    + " | beginningSeq=" + props.getBeginningSequenceNumber()
                                    + " | lastEnqueuedSeq=" + props.getLastEnqueuedSequenceNumber()
                                    + " | lastEnqueuedOffset=" + props.getLastEnqueuedOffset()
                                    + " | lastEnqueuedTime=" + (props.getLastEnqueuedTime() != null ? props.getLastEnqueuedTime() : "null")
                                    + " | isEmpty=" + props.isEmpty()
                    );
                }
            }

        } catch (Exception e) {
            context.getLogger().severe("DescribeEventHubOnSchedule failed: " + e.getMessage());
            throw e; // surfaces failure in Azure Functions monitoring
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    /**
     * Event Hub trigger function:
     * Reads incoming events from the Event Hub (your "topic").
     *
     * This uses identity-based connection via app setting:
     *   EventHubConnection__fullyQualifiedNamespace = <namespace>.servicebus.windows.net
     *
     * And these settings:
     *   EVENTHUB_NAME
     *   EVENTHUB_CONSUMER_GROUP
     */
    @FunctionName("ReadEventHubEvents")
    public void readEventHubEvents(
            @EventHubTrigger(
                    name = "message",
                    eventHubName = "%EVENTHUB_NAME%",
                    consumerGroup = "%EVENTHUB_CONSUMER_GROUP%",
                    connection = "EventHubConnection",
                    cardinality = Cardinality.MANY
            )
            String[] messages,
            final ExecutionContext context) {

        for (String msg : messages) {
            context.getLogger().info("EventHub message: " + msg);
        }
    }
}
