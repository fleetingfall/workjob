package sparkDM.firstwork;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;
import java.util.UUID;

/**
* @author kingcall
* @create 2017-12-11 12:15
* Describe
**/

public class SendMessage {
    public static Random rand=new Random();
    /*一些特定的host*/
    public static String[] hostname={"api.longzhu.com","api.plu.cn","betapi.longzhu.com","configapi.longzhu.com","event-api.longzhu.com","giftapi.plu.cn",
            "id-api.longzhu.com","login.plu.cn","mb.tga.plu.cn"};

    public static void main(String[] args) {
       sendMessage();

    }

    public static void sendMessage(){
        KafkaProducer<String,String> producer=KafkaUtil.getProducer("master",9092);
        long cnt=1;
        System.out.println("=========================即将发送消息==========================");
        while (true){
            //消息躰,record的构造方法开可以再加一个参数，也就是第二个，是分区。
            String value=createMessage();
            ProducerRecord<String,String> message=new ProducerRecord<>("direcrDM",String.valueOf(cnt),value);
            producer.send(message);
            System.out.println(cnt);
            cnt++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    /*
    x_forwarded_for     	string              	客户端IP
    is_blocked          	int                 	是否被openresty屏蔽 1屏蔽 0未屏蔽
    args                	string              	http参数
    status              	int                 	http状态
    cookie              	string              	cookie
    request_timestamp   	bigint              	请求时间戳(ms)
    referer             	string              	访问来源
    host                	string              	域名
    method              	string              	http方法
    scheme              	string              	请求类型
    response_time       	bigint              	响应时间（ms）
    response            	string              	响应体
    user_agent          	string
    body                	string              	post方法请求体
    uri                 	string              	请求uri
    client_ip           	string              	客户端IP（备用）
    uid                 	bigint              	user_id
    uuid                	string              	uuid
    hit                 	string              	内存命中机制
    */
    /*
    year                	string
    month               	string
    day                 	string
    hour                	string
    domain_host         	string
    * */
    public static String createMessage(){
        String client_ip= rand.nextInt(9)+""+rand.nextInt(9)+""+rand.nextInt(9)+"."+rand.nextInt(9)+""+rand.nextInt(9)+""+rand.nextInt(9)+"."+rand.nextInt(9)+""+rand.nextInt(9)+""
                +rand.nextInt(9)+"."+rand.nextInt(9)+""+rand.nextInt(9)+""+rand.nextInt(9);
        String is_blocked="1";
        String args="kingcall";
        String status="200";
        String uid = UUID.randomUUID().toString().replaceAll("-", "");
        String host=hostname[rand.nextInt(hostname.length)];
        OriginalMessageBean bean=new OriginalMessageBean(client_ip,is_blocked,args,status,uid,host,String.valueOf(System.currentTimeMillis()));
        return JSON.toJSONString(bean);
    }
}
