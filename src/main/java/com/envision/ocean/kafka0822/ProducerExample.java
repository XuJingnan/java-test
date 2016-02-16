package com.envision.ocean.kafka0822;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

public class ProducerExample {

    private static final String str10B, str100B, str1K, str10K, str100K;

    static {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            builder.append("*");
        }
        str10B = builder.toString();

        builder.setLength(0);
        for (int i = 0; i < 10; i++) {
            builder.append(str10B);
        }
        str100B = builder.toString();

        builder.setLength(0);
        for (int i = 0; i < 10; i++) {
            builder.append(str100B);
        }
        str1K = builder.toString();

        builder.setLength(0);
        for (int i = 0; i < 10; i++) {
            builder.append(str1K);
        }
        str10K = builder.toString();

        builder.setLength(0);
        for (int i = 0; i < 10; i++) {
            builder.append(str10K);
        }
        str100K = builder.toString();
    }

    /*
        java -cp java-test-1.0-SNAPSHOT.jar com.envision.ocean.kafka0822.ProducerExample 3000000 10.21.10.119:9092 1K topic-1p

        args[0]:    logNumber
        args[1]:    broker.list
        args[2]:    messageSize
        args[3]:    topic
     */
    public static void main(String[] args) {
        long logNumber = Long.parseLong(args[0]);
        Properties props = new Properties();
        props.put("metadata.broker.list", args[1]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.envision.ocean.kafka0822.SimplePartitioner");
        props.put("producer.type", "async");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        String msg = args[2].equals("10B") ? str10B :
                args[2].equals("100B") ? str100B :
                        args[2].equals("1K") ? str1K :
                                args[2].equals("10K") ? str10K : str100K;

        long startTime = new Date().getTime();
        long endTime;
        for (long index = 1; index <= logNumber; index++) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(args[3], "" + index, msg);
            producer.send(data);
            if (index % 10000 == 0) {
                endTime = new Date().getTime();
                System.out.println(index + "th message, QPS " + index * 1000.0 / (endTime - startTime));
            }
        }
        producer.close();
    }
}