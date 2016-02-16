package com.envision.ocean.kafka0822;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroupExample {
    private final ConsumerConnector consumerConnector;
    private final String topic;
    private ExecutorService executor;

    public ConsumerGroupExample(String zookeeper, String groupId, String topic) {
        consumerConnector = Consumer.createJavaConsumerConnector(
                createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
    }

    public void shutdown() {
        if (consumerConnector != null) consumerConnector.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerExample(stream, threadNumber));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    /*
        java -cp java-test-1.0-SNAPSHOT.jar com.envision.ocean.kafka0822.ConsumerGroupExample 10.21.10.119:2181 test topic-3p 1
     */
    public static void main(String[] args) {
        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);
        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic);
        example.run(threads);
        try {
            Thread.sleep(10000000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();
    }
}