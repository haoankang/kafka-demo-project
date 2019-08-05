package ank.hao.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

public class AnkPartitioner implements Partitioner {
    @Override
    /**
     * s: topic name
     * o: key
     * bytes: key bytes
     * o1: value
     * bytes1: value bytes
     * cluster: 集群信息
     */
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
//        String mm = new String(bytes, StandardCharsets.UTF_8);
//        String nn = new String(bytes1, StandardCharsets.UTF_8);
        return new Random().nextInt(cluster.partitionCountForTopic(s));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
