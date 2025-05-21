package com.lucas_hust.kafka.tools.constants;

/**
 * Defines constants used across the Kafka tools application, primarily for Kafka configuration properties.
 */
public interface KafkaConstants {

    // Standard Kafka client configuration keys
    String BOOTSTARP_SERVER = "bootstrap.servers";
    String GROUP_ID = "group.id";
    String KEY_DESERIALIZE = "key.deserializer";
    String VALUE_DESERIALIZE = "value.deserializer";

    // Custom or tool-specific keys (though TOPIC_NAME is often used this way)
    String TOPIC_NAME = "topic.name"; // Used to pass topic name, not a direct Kafka client config
    String START_TIME = "topic.start.time"; // Custom property for tool logic
    String END_TIME = "topic.end.time";     // Custom property for tool logic

}
