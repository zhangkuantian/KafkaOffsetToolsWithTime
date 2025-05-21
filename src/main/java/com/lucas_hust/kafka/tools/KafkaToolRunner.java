package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.constants.KafkaConstants;
import com.lucas_hust.kafka.tools.exception.LoadKafkaOffsetException;
// import org.apache.commons.cli.Options; // No longer needed.
import java.util.Properties;
// import java.util.function.Consumer; // No longer needed.
// import java.util.function.Function; // No longer needed directly

public class KafkaToolRunner {

    /**
     * Functional interface for the core logic of a Kafka tool.
     *
     * @param <R> The return type of the tool's execution.
     */
    @FunctionalInterface
    public interface ToolLogic<R> {
        /**
         * Applies the tool's logic.
         *
         * @param options The parsed command line options.
         * @return The result of the tool's execution.
         * @throws Exception if an error occurs during execution.
         */
        R apply(CliOptions options) throws Exception;
    }

    /**
     * Runs a Kafka tool with the given arguments, logic, and help printer.
     *
     * @param args        The command line arguments.
     * @param toolLogic   The core logic of the tool.
     * @param helpPrinter A {@link Runnable} that prints the help message for the tool.
     * @param <R>         The return type of the tool's execution.
     * @return The result of the tool's execution, or null if help was printed or an error occurred.
     */
    public static <R> R run(String[] args, ToolLogic<R> toolLogic, Runnable helpPrinter) {
        // Initial checks for empty arguments or explicit help flags.
        if (args == null || args.length == 0) {
            helpPrinter.run();
            // Null return indicates to the caller that execution should not proceed.
            return null;
        }

        // Early exit if help option is detected.
        // This avoids unnecessary option parsing if the user only wants help.
        if (args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("--help")) {
            helpPrinter.run();
            return null;
        }

        CliOptions options;
        try {
            // Attempt to parse all command line arguments.
            options = CliOptionsParser.parseEmbeddedModeClient(args);
            // If the parsing itself indicates help is needed (e.g. -h was processed by the parser),
            // print help and exit.
            if (options.isPrintHelp()) {
                helpPrinter.run();
                return null;
            }
        } catch (LoadKafkaOffsetException e) {
            // Handle exceptions specifically related to option parsing (e.g., missing required options).
            System.err.println("Configuration Error: " + e.getMessage()); // Made message more specific
            helpPrinter.run(); // Show help so the user can correct the arguments.
            return null;
        }

        try {
            // Execute the core logic of the specific Kafka tool.
            return toolLogic.apply(options);
        } catch (Exception e) {
            // Catch-all for any other exceptions that occur during the tool's execution.
            System.err.println("Execution Error: An unexpected error occurred while running the tool: " + e.getMessage());
            e.printStackTrace(); // Provides stack trace for debugging. Consider logging this instead in a production tool.
            return null;
        }
    }


    /**
     * Creates a {@link Properties} object for Kafka consumer/producer configuration.
     *
     * @param options The parsed command line options.
     * @return A {@link Properties} object configured with Kafka settings.
     */
    public static Properties createKafkaProperties(CliOptions options) {
        Properties props = new Properties();
        // Essential Kafka properties for connecting and identifying the consumer/client.
        props.setProperty(KafkaConstants.BOOTSTARP_SERVER, options.getBootstrapServer());
        props.setProperty(KafkaConstants.GROUP_ID, options.getGroupId());

        // TOPIC_NAME is not a standard Kafka client property but is used by these tools
        // to internally manage which topic to operate on (e.g., for consumer.partitionsFor()).
        props.setProperty(KafkaConstants.TOPIC_NAME, options.getTopicName());

        // Deserializer classes for message keys and values.
        props.setProperty(KafkaConstants.KEY_DESERIALIZE, options.getKeyDeserialize());
        props.setProperty(KafkaConstants.VALUE_DESERIALIZE, options.getValueDeserialize());
        return props;
    }

}
