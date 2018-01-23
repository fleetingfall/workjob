package scala.firstwork;

import jodd.util.StringUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by Administrator on 2017/12/11.
 * 创造生产者
 */
public class KafkaUtil {
    /**
     * 获取kafka生产者实例
     * @param ip 服务器ip
     * @param port 推送端口
     * @return 已打开的生产者实例
     */

    public static KafkaProducer<String,String> getProducer(String ip,int port){
        if(StringUtil.isEmpty(ip)||port<0){
            System.out.println("输入参数有误，请检查");
            return null;
        }
        //new一个配置文件
        Properties prop = new Properties();
        //推消息连接地址默认端口9092
        //prop.put("bootstrap.servers","master.hadoop:9092");
        
        //bootstrap前端框架
        prop.put("bootstrap.servers",ip+":"+port);
        prop.put("acks", "0");
        prop.put("retries", 0);
        prop.put("batch.size", 16384);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //打开一个生产者
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(prop);
        return kafkaProducer;

    }

    public static KafkaConsumer<String,String> getConsumer(String ip,int port){
        if(StringUtil.isEmpty(ip)||port<0){
            return null;
        }
        //new一个配置文件
        Properties prop = new Properties();
        prop.put("bootstrap.servers",ip+":"+port);

        prop.put("group.id", "12");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String>kafkaConsumer=new KafkaConsumer<String, String>(prop);
        return kafkaConsumer;
    }
}
