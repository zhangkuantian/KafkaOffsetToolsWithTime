package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.constants.KafkaConstants;
import com.lucas_hust.kafka.tools.util.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerKafkaFromTime {

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
        List<TopicPartition> topicPartitions = new ArrayList<>();
        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        for(PartitionInfo partitionInfo:partitionInfos){
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
            topicPartitionLongMap.put(topicPartition, timeStamp);
        }
        consumer.assign(topicPartitions);
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetAndTimes = consumer.offsetsForTimes(topicPartitionLongMap);
        for(Map.Entry<TopicPartition, OffsetAndTimestamp> topicOffsetAndTime:topicOffsetAndTimes.entrySet()){
            System.out.println("Topic: " + topicOffsetAndTime.getKey().topic() + " partition:" +
                    topicOffsetAndTime.getKey().partition() + " timeStamp:" + topicOffsetAndTime.getValue().timestamp() +
                    " offset:" + topicOffsetAndTime.getValue().offset());
            System.out.println();
            consumer.seek(topicOffsetAndTime.getKey(), topicOffsetAndTime.getValue().offset());
        }

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("partition: " + record.partition() + " offset: " + record.offset() + " value: " + record.value() + " key: " + record.key());
            }
        }
    }
}