package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Kafka consumer tool that reads messages from a topic starting from a specified time.
 * This tool runs indefinitely until manually terminated (e.g., via Ctrl+C).
 * A shutdown hook is registered to ensure the Kafka consumer is closed gracefully.
 */
public class ConsumerKafkaFromTime {

    /**
     * Main entry point for the ConsumerKafkaFromTime tool.
     * Uses {@link KafkaToolRunner} to parse arguments and execute the core consumption logic.
     *
     * @param args Command line arguments specifying Kafka connection, topic, and start time.
     */
    public static void main(String[] args) {
        KafkaToolRunner.run(args, options -> {
            // Runner handles null options by printing help.
            if (options == null) {
                return null; // Indicates to runner that execution should not proceed.
            }

            Properties props = KafkaToolRunner.createKafkaProperties(options);
            // The KafkaConsumer is not declared in a try-with-resources block here
            // because the main consumption logic involves an infinite loop.
            // Resource cleanup is handled by a shutdown hook and a finally block.
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            // Register a shutdown hook to close the consumer on JVM termination.
            // This is crucial for releasing resources if the application is stopped externally.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println(
                        "Shutdown hook triggered for ConsumerKafkaFromTime: Attempting to close Kafka consumer..."
                );
                consumer.close(); // Close the consumer.
                System.out.println("Kafka consumer for ConsumerKafkaFromTime closed successfully via shutdown hook.");
            }));

            try {
                // Parse the start time string from options into milliseconds.
                long timeStampMs = Utils.parseStringDate2Long(
                        options.getStartTime(),
                        com.lucas_hust.kafka.tools.util.TimeConstants.YYYY_MM_DD_HH_MI_SS_FORMAT
                );

                // Fetch partition information for the specified topic.
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(options.getTopicName());
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    System.out.println("Warning: No partitions found for topic: " + options.getTopicName() + ". Consumer will not start.");
                    return null;
                }

                List<TopicPartition> topicPartitions = new ArrayList<>();
                Map<TopicPartition, Long> topicPartitionToTimestampMap = new HashMap<>();

                // Prepare a map from TopicPartition to the target timestamp (startTimeMs).
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(
                            partitionInfo.topic(),
                            partitionInfo.partition()
                    );
                    topicPartitions.add(topicPartition);
                    topicPartitionToTimestampMap.put(topicPartition, timeStampMs);
                }
                // Assign consumer to all relevant partitions.
                consumer.assign(topicPartitions);

                // Fetch offsets for the given partitions and timestamps.
                Map<TopicPartition, OffsetAndTimestamp> partitionToOffsetAndTimestampMap =
                        consumer.offsetsForTimes(topicPartitionToTimestampMap);

                // Seek each partition to the determined offset.
                if (partitionToOffsetAndTimestampMap != null) {
                    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry :
                            partitionToOffsetAndTimestampMap.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        OffsetAndTimestamp ots = entry.getValue();
                        if (ots != null) {
                            // If an offset is found, seek to that offset.
                            System.out.println("Seeking Partition: " + tp.partition() +
                                               " to Offset: " + ots.offset() +
                                               " (Timestamp: " + ots.timestamp() + ")");
                            consumer.seek(tp, ots.offset());
                        } else {
                            // If no offset (e.g., startTime is before the first message or after the last),
                            // print a message. The consumer will read based on 'auto.offset.reset'
                            // (typically 'latest' or 'earliest') or current position if already set.
                            // For more predictable behavior, one might explicitly seek to beginning or end.
                            System.out.println("No specific offset found for Partition: " + tp.partition() +
                                               " at Timestamp: " + timeStampMs +
                                               ". Consumer behavior depends on 'auto.offset.reset' or current position.");
                            // Example: consumer.seekToBeginning(Collections.singletonList(tp));
                        }
                    }
                } else {
                     System.out.println("Warning: offsetsForTimes returned null. Could not set initial offsets based on time.");
                }
                

                // Infinite loop to continuously poll for new messages.
                // Termination is expected via an external signal (e.g., Ctrl+C),
                // which will trigger the registered shutdown hook for cleanup.
                System.out.println("Starting message consumption from topic: " + options.getTopicName() + " (Press Ctrl+C to stop)...");
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000); // Poll with a 1-second timeout.
                    // Process each record in the batch.
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(
                                "Partition: " + record.partition() +
                                ", Offset: " + record.offset() +
                                ", Key: " + record.key() +
                                ", Value: " + record.value()
                        );
                    }
                }
            } catch (Exception e) {
                // Catch any other unexpected exceptions during the tool's execution.
                System.err.println("Error during Kafka consumption in ConsumerKafkaFromTime: " + e.getMessage());
                // e.printStackTrace(); // Stack trace can be overwhelming for users; log it instead if possible.
            } finally {
                // This finally block ensures the consumer is closed if the loop terminates
                // due to an exception not caught by the shutdown hook (though unlikely for external termination).
                System.out.println("ConsumerKafkaFromTime finally block: Attempting to close Kafka consumer...");
                consumer.close();
                System.out.println("Kafka consumer for ConsumerKafkaFromTime closed in finally block.");
            }
            // This line is typically unreachable due to the infinite loop.
            // If the loop were to be designed to exit, a meaningful return value might be used.
            return null;
        }, CliOptionsParser::printHelpClient);
    }

}