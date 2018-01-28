package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Authork:kingcall
 * @Description:
 * @Date:$time$ $date$
 */
public class SendMessage {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.53.1.30:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(props);
        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("kingcall", Integer.toString(i), Integer.toString(i)));
            System.out.println("消息 "+i+" 已经被发送");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
