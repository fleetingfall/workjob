package sparkDM.firstwork;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;

/**
* @author kingcall
* @create 2017-12-11 12:15
* Describe
**/

public class GetMessage {
    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer("tencent",9092);
        consumer.subscribe(Arrays.asList("kingcall"));
        consumer.seekToBeginning(new ArrayList<>());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }


    }
}
