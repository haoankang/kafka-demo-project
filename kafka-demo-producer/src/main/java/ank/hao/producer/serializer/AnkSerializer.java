package ank.hao.producer.serializer;

import org.apache.kafka.common.serialization.Serializer;

public class AnkSerializer implements Serializer<String> {
    @Override
    public byte[] serialize(String s, String t) {
        return new byte[0];
    }
}
