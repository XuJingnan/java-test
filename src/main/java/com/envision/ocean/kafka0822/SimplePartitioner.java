package com.envision.ocean.kafka0822;

/**
 * Created by xujingnan on 16-1-31.
 */

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

    public SimplePartitioner(VerifiableProperties props) {
    }

    public int partition(Object key, int a_numPartitions) {
        return Integer.parseInt((String) key) % a_numPartitions;
    }
}