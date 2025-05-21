package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka consumer tool that reads messages from a topic within a specified start and end time range
 * and writes them to an output file (./result.txt).
 */
public class ConsumerKafkaBetweenTime {

    /**
     * Main entry point for the ConsumerKafkaBetweenTime tool.
     * It uses {@link KafkaToolRunner} to parse arguments and execute the core consumption logic.
     *
     * @param args Command line arguments specifying Kafka connection details, topic, and time range.
     */
    public static void main(String[] args) {
        KafkaToolRunner.run(args, options -> {
            // Runner already handles null options by printing help.
            if (options == null) {
                return null;
            }

            // This tool specifically requires an end time to define the consumption range.
            if (options.getEndTime() == null || options.getEndTime().isEmpty()) {
                System.err.println("Error: The --end-time option is required for ConsumerKafkaBetweenTime.");
                CliOptionsParser.printHelpClient(); // Show help if specific requirement is not met.
                return null; // Indicate to runner that execution should not proceed.
            }

            Properties props = KafkaToolRunner.createKafkaProperties(options);
            boolean isFinished = false; // Flag to control the main consumption loop.
            String outputFilePath = "./result.txt"; // Define the output file path. Consider making this configurable.

            // Using try-with-resources to ensure KafkaConsumer and file streams are automatically closed.
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                 FileOutputStream fos = new FileOutputStream(outputFilePath);
                 OutputStreamWriter osw = new OutputStreamWriter(fos, Charset.forName("UTF-8")); // Specify UTF-8 for broader character support.
                 BufferedWriter bw = new BufferedWriter(osw, 1024)) { // Buffered writer for efficient file writing.

                // Parse start and end time strings to milliseconds.
                long startTimeMs = Utils.parseStringDate2Long(options.getStartTime(), com.lucas_hust.kafka.tools.util.TimeConstants.YYYY_MM_DD_HH_MI_SS_FORMAT);
                long endTimeMs = Utils.parseStringDate2Long(options.getEndTime(), com.lucas_hust.kafka.tools.util.TimeConstants.YYYY_MM_DD_HH_MI_SS_FORMAT);

                // Fetch partition information for the given topic.
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(options.getTopicName());
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    System.out.println("Warning: No partitions found for topic: " + options.getTopicName() + ". Nothing to consume.");
                    return null; // Exit if no partitions, as no messages can be read.
                }

                List<TopicPartition> topicPartitions = new ArrayList<>();
                Map<TopicPartition, Long> startTimesForPartitions = new HashMap<>();
                Map<TopicPartition, Long> endTimesForPartitions = new HashMap<>();

                // Prepare data structures for fetching offsets based on time.
                for (PartitionInfo partitionInfo : partitionInfos) {
                    TopicPartition topicPartition = new TopicPartition(
                            partitionInfo.topic(),
                            partitionInfo.partition()
                    );
                    topicPartitions.add(topicPartition);
                    startTimesForPartitions.put(topicPartition, startTimeMs);
                    endTimesForPartitions.put(topicPartition, endTimeMs);
                }
                consumer.assign(topicPartitions); // Assign consumer to all relevant partitions.

                // Get start and end offsets for each partition based on the timestamps.
                Map<TopicPartition, OffsetAndTimestamp> startOffsets =
                        consumer.offsetsForTimes(startTimesForPartitions);
                Map<TopicPartition, OffsetAndTimestamp> endOffsets =
                        consumer.offsetsForTimes(endTimesForPartitions);

                // Stores the target end offset for each partition. Consumption stops once this offset is reached.
                ConcurrentHashMap<Integer, Long> partitionTargetEndOffsets = new ConcurrentHashMap<>();

                // Determine and set start and end offsets for each partition.
                for (TopicPartition tp : topicPartitions) {
                    OffsetAndTimestamp startOts = startOffsets.get(tp);
                    if (startOts != null) {
                        // If a start offset is found, seek the consumer to that offset.
                        System.out.println("Seeking Partition: " + tp.partition() +
                                           " to Start Offset: " + startOts.offset() +
                                           " (Timestamp: " + startOts.timestamp() + ")");
                        consumer.seek(tp, startOts.offset());
                    } else {
                        // If no start offset (e.g., startTime is before the first message), seek to the beginning.
                        System.out.println("No start offset found for Partition: " + tp.partition() +
                                           " at Timestamp: " + startTimeMs +
                                           ". Seeking to beginning of partition.");
                        consumer.seekToBeginning(Collections.singletonList(tp));
                    }

                    OffsetAndTimestamp endOts = endOffsets.get(tp);
                    if (endOts != null) {
                        // If an end offset is found, record it as the target stopping point.
                        System.out.println("Partition: " + tp.partition() +
                                           " will stop at End Offset: " + endOts.offset() +
                                           " (Timestamp: " + endOts.timestamp() + ")");
                        partitionTargetEndOffsets.put(tp.partition(), endOts.offset());
                    } else {
                        // If no end offset (e.g., endTime is beyond the last message), use the actual end of the partition.
                        Map<TopicPartition, Long> currentLogEndOffsets =
                                consumer.endOffsets(Collections.singletonList(tp));
                        Long actualEndOffset = currentLogEndOffsets.get(tp);
                        if (actualEndOffset != null) {
                            System.out.println("No specific offset for endTime " + endTimeMs +
                                               " in Partition: " + tp.partition() +
                                               ". Using actual log end offset: " + actualEndOffset);
                            partitionTargetEndOffsets.put(tp.partition(), actualEndOffset);
                        } else {
                            // This case is unlikely if the partition exists but might occur for empty/new partitions.
                            System.out.println("Warning: Could not determine end offset for Partition: " +
                                               tp.partition() + ". This partition might be skipped or read until manually stopped if it receives messages.");
                        }
                    }
                }
                
                // If no partitions have valid end offsets, there's nothing to consume up to a defined point.
                if (partitionTargetEndOffsets.isEmpty() && !topicPartitions.isEmpty()) {
                    System.out.println(
                        "Warning: No end offsets could be determined for any active partition. No messages will be consumed."
                    );
                    return null;
                }

                // Main consumption loop. Continues until isFinished is true.
                while (!isFinished) {
                    // Poll for records with a timeout.
                    ConsumerRecords<String, String> records = consumer.poll(1000);

                    // Check if all partitions have reached their target end offset and no new records are coming in.
                    if (records.isEmpty() && partitionTargetEndOffsets.isEmpty()) {
                        System.out.println(
                                "No more records to consume and all partitions have reached their target end offsets."
                        );
                        isFinished = true; // Set flag to exit the loop.
                        continue; // Skip to next iteration to check loop condition.
                    }

                    // Process each received record.
                    for (ConsumerRecord<String, String> record : records) {
                        bw.append(record.value()).append("\n"); // Write record value to file.

                        Long targetEndOffset = partitionTargetEndOffsets.get(record.partition());
                        // Check if the current record's offset has reached or passed the target end offset.
                        // The condition `record.offset() >= targetEndOffset - 1` is used because offset
                        // points to the *next* message. So, if current offset is target-1, this is the last message.
                        if (targetEndOffset != null && record.offset() >= targetEndOffset - 1) {
                            System.out.println("Reached target end offset for Partition: " + record.partition());
                            partitionTargetEndOffsets.remove(record.partition()); // Remove partition from tracking.
                        }
                    }
                    bw.flush(); // Flush the buffer to ensure data is written to the file.

                    // If all partitions have reached their end offsets, mark as finished.
                    if (partitionTargetEndOffsets.isEmpty()) {
                        isFinished = true;
                    }
                }
                System.out.println("Finished consuming messages. Output written to: " + outputFilePath);

            } catch (IOException e) {
                // Handle IOExceptions specifically related to file operations.
                System.err.println("Error during file operations for " + outputFilePath + ": " + e.getMessage());
                // e.printStackTrace(); // Optionally log or print stack trace for debugging.
            } catch (Exception e) {
                // Handle any other unexpected exceptions during the tool's execution.
                System.err.println("An unexpected error occurred in ConsumerKafkaBetweenTime: " + e.getMessage());
                e.printStackTrace(); // Print stack trace for debugging unexpected errors.
            }
            return null; // Indicate completion or error to KafkaToolRunner.
        }, CliOptionsParser::printHelpClient);
    }


}
