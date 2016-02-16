package com.envision.ocean.kafka0822;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ConsumerExample implements Runnable {
    private KafkaStream stream;
    private int threadID;
    private int processedMessage;
    private long startTime;
    private long endTime;

    public ConsumerExample(KafkaStream a_stream, int a_threadNumber) {
        threadID = a_threadNumber;
        stream = a_stream;
        processedMessage = 0;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
        startTime = new Date().getTime();
        System.out.println("Thread " + threadID + ": start... " + formatter.format(startTime));
        while (it.hasNext()) {
            it.next();
            processedMessage++;
            if (processedMessage == 1) {
                startTime = new Date().getTime();
            }
            if (processedMessage % 10000 == 0) {
                endTime = new Date().getTime();
                System.out.println("Thread " + threadID + ": " + processedMessage + "th message, QPS "
                        + processedMessage * 1000.0 / (endTime - startTime));
            }
        }
        long endTime = new Date().getTime();
        System.out.println("Thread " + threadID + ": end  ... " + formatter.format(endTime));
    }
}