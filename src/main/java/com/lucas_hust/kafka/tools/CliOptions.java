package com.lucas_hust.kafka.tools;

public class CliOptions {
    private final boolean isPrintHelp;
    private final String bootstrapServer;
    private final String groupId;
    private final String topicName;
    private final String startTime;
    private final String endTime;
    private final String keyDeserialize;
    private final String valueDeserialize;

    public CliOptions(boolean isPrintHelp, String bootstrapServer, String groupId, String topicName, String startTime, String endTime, String keyDeserialize, String valueDeserialize) {
        this.isPrintHelp = isPrintHelp;
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.topicName = topicName;
        this.startTime = startTime;
        this.endTime = endTime;
        this.keyDeserialize = keyDeserialize;
        this.valueDeserialize = valueDeserialize;
    }

    public String getEndTime() {
        return endTime;
    }

    public boolean isPrintHelp() {
        return isPrintHelp;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getKeyDeserialize() {
        return keyDeserialize;
    }

    public String getValueDeserialize() {
        return valueDeserialize;
    }

    @Override
    public String toString() {
        return "CliOptions{" +
                "isPrintHelp=" + isPrintHelp +
                ", bootstrapServer='" + bootstrapServer + '\'' +
                ", groupId='" + groupId + '\'' +
                ", topicName='" + topicName + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", keyDeserialize='" + keyDeserialize + '\'' +
                ", valueDeserialize='" + valueDeserialize + '\'' +
                '}';
    }
}
