package kafka.sendAndreceive;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import sparkDM.firstwork.KafkaUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @Authork:kingcall
 * @Description:
 * @Date:$time$ $date$
 */
public class Getmessage {
    public static KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer("master",9092,"false");
    public static void main(String[] args) {
        simpleConsume();
    }

    /**
     * 简单的消费
     * 但是发现消息的起始位置好像不对
     */
    public static void simpleConsume(){
        KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer("master",9092,"false");
        consumer.subscribe(Arrays.asList("sp2", "bar"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("partion=%d,offset = %d, key = %s, value = %s%n", record.partition(),record.offset(), record.key(), record.value());
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    /**
     * 手动异步提交
     * 但是发现消息的起始位置好像不对
     */
    public static void CommitByManul(){
        consumer.subscribe(Arrays.asList("sp1", "bar"));
        int minCommitSize=10;
        int icount=0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partion=%d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    icount++;
                }
                if (icount>=minCommitSize){
                    consumer.commitAsync(
                            new OffsetCommitCallback() {
                                @Override
                                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                    if (null==e){
                                        System.out.println("偏移量提交成功，可以继续后续处理");
                                    }else {
                                        System.out.println("偏移量提交失败，请采取相应措施");
                                    }
                                }
                            }
                    );
                    icount=0;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

}