package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.exception.LoadKafkaOffsetException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Parses command line options for the Kafka tools.
 * This class defines the available CLI options and provides methods to parse them.
 */
public class CliOptionsParser {

    // Defines the help option (-h, --help).
    public static final Option OPTION_HELP = Option.builder("h")
            .required(false)
            .longOpt("help")
            .desc("Show the help message with descriptions of all options.")
            .build();

    // Defines the topic name option (--topic).
    public static final Option TOPIC_NAME = Option.builder("topic")
            .required(true)
            .longOpt("topic")
            .numberOfArgs(1)
            .argName("topic name")
            .desc("The Kafka topic name (required).")
            .build();

    // Defines the end time option (--etime).
    public static final Option END_TIME = Option.builder("etime")
            .required(false) // Not required by default, specific tools may enforce it.
            .longOpt("end-time")
            .numberOfArgs(1)
            .argName("end-time")
            .desc("End time for consuming messages (optional, format: YYYY-MM-dd HH:mm:SS).")
            .build();

    // Defines the bootstrap server option (--server).
    public static final Option BOOTSTARP_SERVER = Option.builder("server")
            .required(true)
            .longOpt("bootstarp-server")
            .numberOfArgs(1)
            .argName("bootstarp-server list")
            .desc("Kafka bootstrap server list (e.g., localhost:9092) (required).")
            .build();

    // Defines the group ID option (--gid).
    public static final Option GROUP_ID = Option.builder("gid")
            .required(true)
            .longOpt("group-id")
            .numberOfArgs(1)
            .argName("group-id name")
            .desc("Kafka consumer group ID (required).")
            .build();

    // Defines the start time option (--stime).
    public static final Option START_TIME = Option.builder("stime")
            .required(true)
            .longOpt("start-time")
            .numberOfArgs(1)
            .argName("start-time") // Corrected: Was "group-id name"
            .desc("Start time for consuming messages (required, format: YYYY-MM-dd HH:mm:SS).") // Corrected: Was "a group-id name must enter"
            .build();

    // Defines the key deserializer option (--keydes).
    public static final Option KEY_DESERIALIZE = Option.builder("keydes")
            .required(false)
            .longOpt("key-deserialize")
            .numberOfArgs(1)
            .argName("key.deserialize class name")
            .desc("Key deserializer class (default: org.apache.kafka.common.serialization.StringDeserializer).")
            .build();

    // Defines the value deserializer option (--valuedes).
    public static final Option VALUE_DESERIALIZE = Option.builder("valuedes")
            .required(false)
            .longOpt("value-deserialize")
            .numberOfArgs(1)
            .argName("value.deserialize class name") // Corrected: Was "key.deserialize name"
            .desc("Value deserializer class (default: org.apache.kafka.common.serialization.StringDeserializer).") // Corrected: Was "a key.deserialize name must enter"
            .build();

    /**
     * Prints the help message for the Kafka tools to standard output.
     * This typically shows a general usage pattern and then detailed options.
     */
    public static void printHelpClient() {
        // General usage instruction.
        System.out.println("./LoadKafkaOffsetWithTimeStamp [OPTIONS]"); // TODO: Consider making the script name dynamic or more generic.
        System.out.println();
        System.out.println("The following options are available:");
        // Prints the detailed options formatted by HelpFormatter.
        printHelpEmbeddedModeClient();
        System.out.println();
    }

    /**
     * Prints the detailed options description using {@link HelpFormatter}.
     * This method configures the formatter and displays the help for the tool's options.
     */
    public static void printHelpEmbeddedModeClient() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5); // Padding for option descriptions.
        formatter.setWidth(120); // Max width for the help output. Consider making this a constant.
        // Example syntax line.
        System.out.println("\n  Syntax: LoadKafkaOffsetWithTimeStamp [OPTIONS]"); // TODO: Consider making the script name dynamic or more generic.
        formatter.setSyntaxPrefix("  \"LoadKafkaOffsetWithTimeStamp\" options:"); // Prefix for the options list.
        // Generates and prints the help text for the options.
        formatter.printHelp(" ", getEmbeddedModeClientOptions(new Options()));
        System.out.println();
    }

    /**
     * Adds general options (like help) to the provided {@link Options} object.
     * @param options The {@link Options} object to which general options will be added.
     */
    private static void buildGeneralOptions(Options options) {
        options.addOption(OPTION_HELP);
    }

    /**
     * Constructs and returns an {@link Options} object containing all defined CLI options
     * for the Kafka tools.
     * @param options An existing {@link Options} object to which the tool-specific options will be added.
     *                A new {@link Options} object will be used if this is null.
     * @return An {@link Options} object configured with all CLI options.
     */
    public static Options getEmbeddedModeClientOptions(Options options) {
        if (options == null) {
            options = new Options();
        }
        buildGeneralOptions(options);
        // Add all tool-specific options.
        options.addOption(TOPIC_NAME);
        options.addOption(BOOTSTARP_SERVER);
        options.addOption(GROUP_ID);
        options.addOption(END_TIME);
        options.addOption(START_TIME);
        options.addOption(KEY_DESERIALIZE);
        options.addOption(VALUE_DESERIALIZE);
        return options;
    }

    /**
     * Parses the given command line arguments and constructs a {@link CliOptions} object.
     *
     * @param args The command line arguments to parse.
     * @return A {@link CliOptions} object populated with the parsed values.
     * @throws LoadKafkaOffsetException if parsing fails or if required options are missing.
     */
    public static CliOptions parseEmbeddedModeClient(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            // Parse the arguments; 'true' allows partial matching if unambiguous.
            CommandLine line = parser.parse(getEmbeddedModeClientOptions(new Options()), args, true);

            // Construct CliOptions from the parsed command line.
            return new CliOptions(
                    line.hasOption(OPTION_HELP.getOpt()),
                    checkBootstrapServer(line),
                    checkGroupId(line),
                    checkTopicName(line),
                    checkStartTime(line),
                    line.getOptionValue(END_TIME.getOpt()), // endTime is optional, so getOptionValue is fine.
                    // Provide default values for optional deserializer options.
                    line.getOptionValue(KEY_DESERIALIZE.getOpt(), "org.apache.kafka.common.serialization.StringDeserializer"),
                    line.getOptionValue(VALUE_DESERIALIZE.getOpt(), "org.apache.kafka.common.serialization.StringDeserializer")
            );
        } catch (ParseException e) {
            // KafkaToolRunner handles printing help when this exception is caught.
            // Removed commented-out printHelpEmbeddedModeClient() call.
            throw new LoadKafkaOffsetException("Error parsing command line arguments: " + e.getMessage(), e);
        }
    }

    /**
     * Checks for and returns the start time from the parsed command line.
     * Throws {@link LoadKafkaOffsetException} if the start time is missing.
     *
     * @param line The parsed {@link CommandLine}.
     * @return The start time string.
     */
    public static String checkStartTime(CommandLine line) {
        final String startTime = line.getOptionValue(START_TIME.getOpt());
        if (startTime == null || startTime.isEmpty()) {
            throw new LoadKafkaOffsetException("Required option --start-time (-stime) is missing.");
        }
        return startTime;
    }

    // No checkEndTime method as endTime is optional.
    // If specific validation for endTime (other than presence) were needed, it could be added here.

    /**
     * Checks for and returns the topic name from the parsed command line.
     * Throws {@link LoadKafkaOffsetException} if the topic name is missing.
     *
     * @param line The parsed {@link CommandLine}.
     * @return The topic name string.
     */
    public static String checkTopicName(CommandLine line) {
        final String topic = line.getOptionValue(TOPIC_NAME.getOpt());
        if (topic == null || topic.isEmpty()) {
            throw new LoadKafkaOffsetException("Required option --topic is missing.");
        }
        return topic;
    }

    /**
     * Checks for and returns the group ID from the parsed command line.
     * Throws {@link LoadKafkaOffsetException} if the group ID is missing.
     *
     * @param line The parsed {@link CommandLine}.
     * @return The group ID string.
     */
    public static String checkGroupId(CommandLine line) {
        final String groupId = line.getOptionValue(GROUP_ID.getOpt());
        if (groupId == null || groupId.isEmpty()) {
            throw new LoadKafkaOffsetException("Required option --group-id (-gid) is missing.");
        }
        return groupId;
    }

    /**
     * Checks for and returns the bootstrap server list from the parsed command line.
     * Throws {@link LoadKafkaOffsetException} if the bootstrap server list is missing.
     *
     * @param line The parsed {@link CommandLine}.
     * @return The bootstrap server list string.
     */
    public static String checkBootstrapServer(CommandLine line) {
        final String bootstrapServer = line.getOptionValue(BOOTSTARP_SERVER.getOpt());
        if (bootstrapServer == null || bootstrapServer.isEmpty()) {
            throw new LoadKafkaOffsetException("Required option --bootstarp-server (-server) is missing.");
        }
        return bootstrapServer;
    }

}
