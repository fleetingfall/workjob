package sparkDM.firstwork;

import jodd.util.StringUtil;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Properties;

/**
 * Created by Administrator on 2017/12/11.
 * 创造生产者
 */
public class KafkaUtil {
    static final String ZK_CONNNECT="master:2181,slave1:2181,slave2:2181";
    static final int SESSION_TIMEOUT=30000;
    static final int CONNECT_TIMEOUT=30000;
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

    public static KafkaConsumer<String,String> getConsumer(String ip,int port,String autoCommit){
        if(StringUtil.isEmpty(ip)||port<0){
            return null;
        }
        //new一个配置文件
        Properties prop = new Properties();
        prop.put("bootstrap.servers",ip+":"+port);

        prop.put("group.id", "12");
        //是否自动提交
        prop.put("enable.auto.commit", autoCommit);
        //多长事件提交一次offset
        if (autoCommit.equals("true")){
            System.out.println("offset 提交时间间隔未设置");
        }else {
            prop.put("enable.auto.commit", "true");
            prop.put("auto.commit.interval.ms", "1000");
        }
        prop.put("session.timeout.ms", "30000");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String>kafkaConsumer=new KafkaConsumer<String, String>(prop);
        return kafkaConsumer;
    }

    /**
     * 新版本才可使用
     */

    public static void CreateTopic(String topic,int partion,int replica,Properties properties){
      /*  ZkUtils zk=null;
        try {
            zk= ZkUtils.apply(ZK_CONNNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
            if (!AdminUtils.topicExists(zk,topic)){
                AdminUtils.createTopic(zk,topic,partion,replica,properties,AdminUtils.createTopic$default$6());
            }else{
                System.out.println("主题已经存在");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            zk.close();
        }*/
    }
}
