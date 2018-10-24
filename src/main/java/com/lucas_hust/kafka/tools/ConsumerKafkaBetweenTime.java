package com.lucas_hust.kafka.tools;

import com.lucas_hust.kafka.tools.constants.KafkaConstants;
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

public class ConsumerKafkaBetweenTime {
    public static void main(String []args){
        if(0 >= args.length){
            BetweenCliOptionsParser.printHelpClient();
            return;
        }

        if(args[0].equalsIgnoreCase("-h")){
            BetweenCliOptionsParser.printHelpClient();
            return;
        }

        CliOptions options = BetweenCliOptionsParser.parseEmbeddedModeClient(args);
        if(options.isPrintHelp()){
            BetweenCliOptionsParser.printHelpClient();
        }
        Properties props = new Properties();
        props.setProperty(KafkaConstants.BOOTSTARP_SERVER, options.getBootstrapServer());
        props.setProperty(KafkaConstants.GROUP_ID, options.getGroupId());
        props.setProperty(KafkaConstants.TOPIC_NAME, options.getTopicName());
        props.setProperty(KafkaConstants.KEY_DESERIALIZE, options.getKeyDeserialize());
        props.setProperty(KafkaConstants.VALUE_DESERIALIZE, options.getValueDeserialize());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        boolean isFinished = false;

        long startTime = Utils.parseStringDate2Long(options.getStartTime(), "YYYY-MM-dd HH:mm:SS");
        long endTime = Utils.parseStringDate2Long(options.getEndTime(), "YYYY-MM-dd HH:mm:SS");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(props.getProperty(KafkaConstants.TOPIC_NAME));
        List<TopicPartition> topicPartitions = new ArrayList<>();
        Map<TopicPartition, Long> topicPartitionLongMap = new HashMap<>();
        Map<TopicPartition, Long> topicPartitionEndMap = new HashMap<>();

        for(PartitionInfo partitionInfo:partitionInfos){
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
            topicPartitionLongMap.put(topicPartition, startTime);
            topicPartitionEndMap.put(topicPartition, endTime);
        }
        consumer.assign(topicPartitions);
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetStartTimes = consumer.offsetsForTimes(topicPartitionLongMap);
        Map<TopicPartition, OffsetAndTimestamp> topicOffsetEndTimes = consumer.offsetsForTimes(topicPartitionEndMap);
//        Map<Integer, Long> partitionOffsets = new HashMap<>();   //fuck! Concurrent Conflicted, replace with ConcurrentHashMap<Integer, Long>!
        ConcurrentHashMap<Integer, Long> partitionOffsets = new ConcurrentHashMap<>();

        for(Map.Entry<TopicPartition, OffsetAndTimestamp> topicOffsetStartTime:topicOffsetStartTimes.entrySet()){
            System.out.println("Topic: " + topicOffsetStartTime.getKey().topic() + " partition:" +
                    topicOffsetStartTime.getKey().partition() + " startTime:" + topicOffsetStartTime.getValue().timestamp() +
                    " offset:" + topicOffsetStartTime.getValue().offset());
            consumer.seek(topicOffsetStartTime.getKey(), topicOffsetStartTime.getValue().offset());
        }

        for(Map.Entry<TopicPartition, OffsetAndTimestamp> topicOffsetEndTime:topicOffsetEndTimes.entrySet()){
            System.out.println("Topic: " + topicOffsetEndTime.getKey().topic() + " partition:" +
                    topicOffsetEndTime.getKey().partition() + " endTime:" + topicOffsetEndTime.getValue().timestamp() +
                    " offset:" + topicOffsetEndTime.getValue().offset());
            partitionOffsets.put(topicOffsetEndTime.getKey().partition(), topicOffsetEndTime.getValue().offset());
        }

        try(FileOutputStream fos = new FileOutputStream("./result.txt");
            OutputStreamWriter osw = new OutputStreamWriter(fos, Charset.forName("UTF-8"));
            BufferedWriter bw = new BufferedWriter(osw, 1024)){
            while(!isFinished){
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("partition: " + record.partition() + " offset: " + record.offset() + " value: " + record.value() + " key: " + record.key());
                    bw.append(record.value() + "\n");
                    bw.flush();
                    for(Map.Entry<Integer, Long> partitionOffset:partitionOffsets.entrySet()){
                        if(record.partition() == partitionOffset.getKey() && record.offset() == partitionOffset.getValue()){
                            partitionOffsets.remove(record.partition());
                        }
                    }
                    isFinished = partitionOffsets.isEmpty();
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
