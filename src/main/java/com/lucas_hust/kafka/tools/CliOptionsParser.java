package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.exception.LoadKafkaOffsetException;
import org.apache.commons.cli.*;

public class CliOptionsParser {

    public static final Option OPTION_HELP = Option
            .builder("h")
            .required(false)
            .longOpt("help")
            .desc("Show the help message with descriptions of all options.")
            .build();
    public static final Option TOPIC_ANME = Option
            .builder("topic")
            .required(true)
            .longOpt("topic")
            .numberOfArgs(1)
            .argName("topic name")
            .desc("a topic name must enter")
            .build();

    public static final Option BOOTSTARP_SERVER = Option
            .builder("server")
            .required(true)
            .longOpt("bootstarp-server")
            .numberOfArgs(1)
            .argName("bootstarp-server list")
            .desc("a bootstarp-server list must enter")
            .build();

    public static final Option GROUP_ID = Option
            .builder("gid")
            .required(true)
            .longOpt("group-id")
            .numberOfArgs(1)
            .argName("group-id name")
            .desc("a group-id name must enter")
            .build();
    public static final Option START_TIME = Option
            .builder("stime")
            .required(true)
            .longOpt("start-time")
            .numberOfArgs(1)
            .argName("group-id name")
            .desc("a group-id name must enter")
            .build();

    public static final Option KEY_DESERIALIZE = Option
            .builder("keydes")
            .required(false)
            .longOpt("key-deserialize")
            .numberOfArgs(1)
            .argName("key.deserialize class name")
            .desc("a key.deserialize class name must enter")
            .build();

    public static final Option VALUE_DESERIALIZE = Option
            .builder("valuedes")
            .required(false)
            .longOpt("value-deserialize")
            .numberOfArgs(1)
            .argName("key.deserialize name")
            .desc("a key.deserialize name must enter")
            .build();

    public static void printHelpClient() {
        System.out.println("./LoadKafkaOffsetWithTimeStamp [OPTIONS]");
        System.out.println();
        System.out.println("The following options are available:");

        printHelpEmbeddedModeClient();

        System.out.println();
    }

    public static void printHelpEmbeddedModeClient() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);
        System.out.println("\n  Syntax: LoadKafkaOffsetWithTimeStamp [OPTIONS]");
        formatter.setSyntaxPrefix("  \"LoadKafkaOffsetWithTimeStamp\" options:");
        formatter.printHelp(" ", getEmbeddedModeClientOptions(new Options()));

        System.out.println();
    }
    private static void buildGeneralOptions(Options options) {
        options.addOption(OPTION_HELP);
    }

    public static Options getEmbeddedModeClientOptions(Options options) {
        buildGeneralOptions(options);
        options.addOption(TOPIC_ANME);
        options.addOption(BOOTSTARP_SERVER);
        options.addOption(GROUP_ID);
        options.addOption(START_TIME);
        options.addOption(KEY_DESERIALIZE);
        options.addOption(VALUE_DESERIALIZE);
        return options;
    }

    public static CliOptions parseEmbeddedModeClient(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(getEmbeddedModeClientOptions(new Options()), args, true);
            return new CliOptions(
                    line.hasOption(CliOptionsParser.OPTION_HELP.getOpt()),
                    checkBootstrapServer(line),
                    checkGroupId(line),
                    checkTopicName(line),
                    checkStartTime(line),
                    null,
                    line.getOptionValue(CliOptionsParser.KEY_DESERIALIZE.getOpt(), "org.apache.kafka.common.serialization.StringDeserializer"),
                    line.getOptionValue(CliOptionsParser.VALUE_DESERIALIZE.getOpt(), "org.apache.kafka.common.serialization.StringDeserializer"));
        }
        catch (ParseException e) {
//            printHelpEmbeddedModeClient();
            throw new LoadKafkaOffsetException(e.getMessage());
        }
    }

    public static String checkStartTime(CommandLine line){
        final String startTime = line.getOptionValue(CliOptionsParser.START_TIME.getOpt());
        if(null == startTime|| startTime.isEmpty()){
            throw new LoadKafkaOffsetException("Kafka Consumer' Start Time Must Enter");
        }
        return startTime;
    }

    public static String checkTopicName(CommandLine line){
        final String topic = line.getOptionValue(CliOptionsParser.TOPIC_ANME.getOpt());
        if(null == topic || topic.isEmpty()){
            throw new LoadKafkaOffsetException("Kafka Topic Must Enter!");
        }
        return topic;
    }

    public static String checkGroupId(CommandLine line){
        final String groupId = line.getOptionValue(CliOptionsParser.GROUP_ID.getOpt());
        if(null == groupId || groupId.isEmpty()){
            throw new LoadKafkaOffsetException(" Kafka Consumer's GroupId Must Enter! ");
        }
        return groupId;
    }

    public static String checkBootstrapServer(CommandLine line){
        final String bootstrapServer = line.getOptionValue(CliOptionsParser.BOOTSTARP_SERVER.getOpt());
        if(null == bootstrapServer || bootstrapServer.isEmpty()){
            throw new LoadKafkaOffsetException("a bootstarp-server list must enter");
        }
        return bootstrapServer;
    }

}
