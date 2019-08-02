package ank.hao.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","10.100.1.130:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "-1");
        properties.put("retries", 2);
        properties.put("batch.size", 323840);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);
        properties.put("max.block.ms", 3000);

        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<3;i++){
            producer.send(new ProducerRecord<String, String>("first-topic", Integer.toString(i), "message-"+Integer.toString(i)));
        }
        for(int m=10;m<3;m++){
            producer.send(new ProducerRecord<String, String>("first-topic", Integer.toString(m), "callback-" + Integer.toString(m)), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        //消息发送成功
                        System.out.println("发送成功了喵："+recordMetadata.topic());
                    }else {
                        if(e instanceof RetriableException){
                            //处理可重试瞬时异常
                        } else {
                            //处理不可重试异常
                        }
                    }
                }
            });
        }
        for(int m=20;m<3;m++){
            try {
                producer.send(new ProducerRecord<String, String>("first-topic", Integer.toString(m), "sync-" + Integer.toString(m))).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
