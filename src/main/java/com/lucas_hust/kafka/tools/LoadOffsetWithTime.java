package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.constants.KafkaConstants;
import com.lucas_hust.kafka.tools.util.Utils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LoadOffsetWithTime {

    public static void main(String []args){
        if(0 >= args.length){
            CliOptionsParser.printHelpClient();
            return;
        }
        if(args[0].equalsIgnoreCase("-h")){
            BetweenCliOptionsParser.printHelpClient();
            return;
        }
        CliOptions options = CliOptionsParser.parseEmbeddedModeClient(args);
        if(options.isPrintHelp()){
            CliOptionsParser.printHelpClient();
        }

        Properties props = new Properties();
        props.setProperty(KafkaConstants.BOOTSTARP_SERVER, options.getBootstrapServer());
        props.setProperty(KafkaConstants.GROUP_ID, options.getGroupId());
        props.setProperty(KafkaConstants.TOPIC_NAME, options.getTopicName());
        props.setProperty(KafkaConstants.KEY_DESERIALIZE, options.getKeyDeserialize());
        props.setProperty(KafkaConstants.VALUE_DESERIALIZE, options.getValueDeserialize());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        long timeStamp = Utils.parseStringDate2Long(options.getStartTime(), "YYYY-MM-dd HH:mm:SS");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(props.getProperty(KafkaConstants.TOPIC_NAME));
        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        for(PartitionInfo partitionInfo:partitionInfos){
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitionLongMap.put(topicPartition, timeStamp);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetAndTimes = consumer.offsetsForTimes(topicPartitionLongMap);
        for(Map.Entry<TopicPartition, OffsetAndTimestamp> topicOffsetAndTime:topicOffsetAndTimes.entrySet()){
            System.out.println("Topic: " + topicOffsetAndTime.getKey().topic() + " partition:" +
                    topicOffsetAndTime.getKey().partition() + " timeStamp:" + topicOffsetAndTime.getValue().timestamp() +
                    " offset:" + topicOffsetAndTime.getValue().offset());
            System.out.println();
        }
        consumer.close();
    }
}