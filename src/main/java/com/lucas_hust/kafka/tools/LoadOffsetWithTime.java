package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.util.Utils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka tool to load and display the offset and timestamp for each partition of a topic
 * corresponding to a given start time.
 */
public class LoadOffsetWithTime {

    /**
     * Main entry point for the LoadOffsetWithTime tool.
     * Uses {@link KafkaToolRunner} to parse arguments and execute the core logic.
     *
     * @param args Command line arguments specifying Kafka connection, topic, and start time.
     */
    public static void main(String[] args) {
        KafkaToolRunner.run(args, options -> {
            // Runner handles null options by printing help and returning null from options.
            if (options == null) {
                return null; // Indicates to runner that execution should not proceed.
            }

            Properties props = KafkaToolRunner.createKafkaProperties(options);
            // Using try-with-resources to ensure the KafkaConsumer is closed automatically.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                // Parse the start time string from options into milliseconds.
                long timeStampMs = Utils.parseStringDate2Long(
                        options.getStartTime(),
                        com.lucas_hust.kafka.tools.util.TimeConstants.YYYY_MM_DD_HH_MI_SS_FORMAT
                );

                // Fetch partition information for the specified topic.
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(options.getTopicName());
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    System.out.println("Warning: No partitions found for topic: " + options.getTopicName() + ". Cannot load offsets.");
                    return null;
                }

                Map<TopicPartition, Long> topicPartitionToTimestampMap = new HashMap<>();
                // Prepare a map from TopicPartition to the target timestamp (startTimeMs).
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(
                            partitionInfo.topic(),
                            partitionInfo.partition()
                    );
                    topicPartitionToTimestampMap.put(topicPartition, timeStampMs);
                }

                // Fetch offsets for the given partitions and timestamps.
                Map<TopicPartition, OffsetAndTimestamp> partitionToOffsetAndTimestampMap =
                        consumer.offsetsForTimes(topicPartitionToTimestampMap);

                // Iterate through the results and print them.
                if (partitionToOffsetAndTimestampMap != null) {
                    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry :
                            partitionToOffsetAndTimestampMap.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        OffsetAndTimestamp ots = entry.getValue();
                        if (ots != null) {
                            // If an offset is found for the timestamp.
                            System.out.println("Topic: " + tp.topic() +
                                               ", Partition: " + tp.partition() +
                                               ", Timestamp: " + ots.timestamp() + // Actual timestamp of the message found
                                               ", Offset: " + ots.offset());
                        } else {
                            // If no offset is found (e.g., the timestamp is out of range for the partition).
                            System.out.println("Topic: " + tp.topic() +
                                               ", Partition: " + tp.partition() +
                                               ", Timestamp: N/A (no offset found for the given start time: " + timeStampMs + ")" +
                                               ", Offset: N/A");
                        }
                    }
                } else {
                    System.out.println("Warning: offsetsForTimes returned null. Could not retrieve any offset information.");
                }
            } catch (Exception e) {
                // Catch any other unexpected exceptions during the tool's execution.
                System.err.println("Error during offset loading in LoadOffsetWithTime: " + e.getMessage());
                e.printStackTrace(); // Print stack trace for debugging.
            }
            return null; // Indicate completion or error to KafkaToolRunner.
        }, CliOptionsParser::printHelpClient);
    }

}