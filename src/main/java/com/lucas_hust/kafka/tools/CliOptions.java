package com.lucas_hust.kafka.tools;

/**
 * Represents the parsed command line options for the Kafka tools.
 * This class is a simple data holder (POJO) for the options.
 */
public class CliOptions {

    private final boolean isPrintHelp;
    private final String bootstrapServer;
    private final String groupId;
    private final String topicName;
    private final String startTime;
    private final String endTime;
    private final String keyDeserialize;
    private final String valueDeserialize;

    /**
     * Constructs a new CliOptions object.
     *
     * @param isPrintHelp      True if the help option was requested, false otherwise.
     * @param bootstrapServer  The Kafka bootstrap server list.
     * @param groupId          The Kafka consumer group ID.
     * @param topicName        The Kafka topic name.
     * @param startTime        The start time for message consumption (format: YYYY-MM-dd HH:mm:SS).
     * @param endTime          The end time for message consumption (format: YYYY-MM-dd HH:mm:SS), can be null.
     * @param keyDeserialize   The key deserializer class name.
     * @param valueDeserialize The value deserializer class name.
     */
    public CliOptions(boolean isPrintHelp, String bootstrapServer, String groupId, String topicName,
                      String startTime, String endTime, String keyDeserialize, String valueDeserialize) {
        this.isPrintHelp = isPrintHelp;
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.topicName = topicName;
        this.startTime = startTime;
        this.endTime = endTime;
        this.keyDeserialize = keyDeserialize;
        this.valueDeserialize = valueDeserialize;
    }

    /**
     * Gets the end time for message consumption.
     * @return The end time string, or null if not set.
     */
    public String getEndTime() {
        return this.endTime;
    }

    /**
     * Checks if the help option was requested.
     * @return True if help was requested, false otherwise.
     */
    public boolean isPrintHelp() {
        return this.isPrintHelp;
    }

    /**
     * Gets the Kafka bootstrap server list.
     * @return The bootstrap server list string.
     */
    public String getBootstrapServer() {
        return this.bootstrapServer;
    }

    /**
     * Gets the Kafka consumer group ID.
     * @return The group ID string.
     */
    public String getGroupId() {
        return this.groupId;
    }

    /**
     * Gets the Kafka topic name.
     * @return The topic name string.
     */
    public String getTopicName() {
        return this.topicName;
    }

    /**
     * Gets the start time for message consumption.
     * @return The start time string.
     */
    public String getStartTime() {
        return this.startTime;
    }

    /**
     * Gets the key deserializer class name.
     * @return The key deserializer class name string.
     */
    public String getKeyDeserialize() {
        return this.keyDeserialize;
    }

    /**
     * Gets the value deserializer class name.
     * @return The value deserializer class name string.
     */
    public String getValueDeserialize() {
        return this.valueDeserialize;
    }

    /**
     * Returns a string representation of the CliOptions object.
     * Useful for debugging.
     * @return A string representation of the object.
     */
    @Override
    public String toString() {
        // Using a StringBuilder for efficient string concatenation.
        StringBuilder sb = new StringBuilder("CliOptions{");
        sb.append("isPrintHelp=").append(isPrintHelp);
        sb.append(", bootstrapServer='").append(bootstrapServer).append('\'');
        sb.append(", groupId='").append(groupId).append('\'');
        sb.append(", topicName='").append(topicName).append('\'');
        sb.append(", startTime='").append(startTime).append('\'');
        sb.append(", endTime='").append(endTime).append('\'');
        sb.append(", keyDeserialize='").append(keyDeserialize).append('\'');
        sb.append(", valueDeserialize='").append(valueDeserialize).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
