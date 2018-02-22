package util;

import constant.JedisPooLConf;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Jedis是 Redis java 版本的客户端，提供了完整Redis命令（命令行例一模一样的命令），而Redisson有更多分布式的容器实现。
 */
public class JedisDM {

    public static void main(String[] args) {
        showInfo();
    }
    public static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    public static JedisPool pool = new JedisPool(JedisPooLConf.getJedisPoolConfig(10,5,20,5000), "localhost", 6379);

    public static void showInfo(){
        System.out.println( pool.getNumActive());
        System.out.println( pool.getNumIdle());
        System.out.println(pool.getNumWaiters());
        pool.destroy();
    }
    public static void setString(String key,String value){
        Jedis jedis=pool.getResource();
        jedis.set(key,value);
        //实际上是将 jedis对象还给jedis池
        jedis.close();
    }
    public static String getString(String key){
        Jedis jedis=pool.getResource();
        String value=jedis.get(key);
        jedis.close();
        return value;
    }

    /**
     * 返回的是添加成功地元素个数
     * @param key
     * @param mem
     * @return
     */
    public static long sadd(String key,String... mem){
        Jedis jedis=pool.getResource();
        long ret=jedis.sadd(key,mem);
        jedis.close();
        return ret;
    }

    public static Set<String> getSetMembers(String key){
        Jedis jedis=pool.getResource();
        Set<String> ret=jedis.smembers(key);
        jedis.close();
        return ret;
    }

    public static long zadd(String key,double score,String mem){
        Jedis jedis=pool.getResource();
        long ret=jedis.zadd(key,score,mem);
        jedis.close();
        return ret;
    }
    public static double zIncreBy(String key,int score,String mem){
        Jedis jedis=pool.getResource();
        double ret=jedis.zincrby(key,score,mem);
        jedis.close();
        return ret;
    }

    /**
     * 获取有序集合中某个成员的分数
     * @param key
     * @param mem
     * @return
     */
    public static double zgetScore(String key,String mem){
        Jedis jedis=pool.getResource();
        double ret=jedis.zscore(key,mem);
        jedis.close();
        return ret;
    }

    /**
     * 获取有序集合里面的元素
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static Set<String> getzMembers(String key,long start,long end){
        Jedis jedis=pool.getResource();
        Set<String> ret=jedis.zrange(key,start,end);
        jedis.close();
        return ret;
    }


    public static void setList(String key,String ... value){
        Jedis jedis=pool.getResource();
        jedis.lpush(key,value);
        jedis.close();
    }
    public static List<String> getList(String key, int start, int end){
        Jedis jedis=pool.getResource();
        List<String> list=jedis.lrange(key,start,end);
        jedis.close();
        return list;
    }

    /**
     * 删除特定的一些key
     * @return
     */
    public static long deletekeys(String...keys){
        Jedis jedis=pool.getResource();
        long size=jedis.del(keys);
        jedis.close();
        return size;
    }

    public static int getKeysCount(){
        Jedis jedis=pool.getResource();
        int size=jedis.keys("*").size();
        jedis.close();
        return size;
    }
    public static Set<String> getKeys(){
        Jedis jedis=pool.getResource();
        Set<String> set=jedis.keys("*");
        jedis.close();
        return set;
    }
    public static int getKeysCount(String pattern){
        Jedis jedis=pool.getResource();
        int size=jedis.keys(pattern).size();
        jedis.close();
        return size;
    }
    public static Set<String> getKeys(String pattern){
        Jedis jedis=pool.getResource();
        Set<String> set=jedis.keys(pattern);
        jedis.close();
        return set;
    }

    /**
     * 判断hash是否存在特定的字段
     * @param key
     * @param field
     * @return
     */
    public static boolean Hashexists(String key,String field){
        Jedis jedis=pool.getResource();
        boolean ret=jedis.hexists(key,field);
        jedis.close();
        return ret;
    }

    /**
     * 对于 hash 不存在 可自动创建  对于 key 不存在可自动创建   对于非数字类型会报错
     * @param key
     * @param field
     * @param increment
     * @return
     */
    public static long HashIncreBy(String key,String field,int increment){
        Jedis jedis=pool.getResource();
        long ret=jedis.hincrBy(key,field,increment);
        jedis.close();
        return ret;
    }

    public static void setHash(String key, Map<String,String> map){
        Jedis jedis=pool.getResource();
        Set<String> keys=map.keySet();
        for (String k:keys){
            jedis.hset(key,k,map.get(k));
        }
        jedis.close();
    }
    public static long setHash(String key, String filed,String value){
        Jedis jedis=pool.getResource();
        long ret=jedis.hset(key,filed,value);
        jedis.close();
        return  ret;
    }

    /**
     * 删除hash的某些字段
     * @param key
     * @param fileds
     * @return
     */
    public static long delHashField(String key,String ...fileds){
        Jedis jedis=pool.getResource();
        long ret=jedis.hdel(key,fileds);
        jedis.close();
        return  ret;
    }

    /**
     * 至少都会返回一个空的map  map.isEmpty()  用这个方法来判断
     * @param key
     * @return
     */
    public static Map<String, String> getHash(String key){
        Jedis jedis=pool.getResource();
        Map<String,String> map=jedis.hgetAll(key);
        jedis.close();
        return map;
    }
    /**
     * 可能返回一个 null的值
     * @param key
     * @return
     */
    public static String getHash(String key,String field){
        Jedis jedis=pool.getResource();
        String result=jedis.hget(key,field);
        jedis.close();
        return result;
    }


    /**
     * 设置key的存活时间是多长，单位是秒
     * @param key
     * @param seconds
     */
    public static void setExpire(String key,int seconds){
        Jedis jedis=pool.getResource();
        jedis.expire(key,seconds);//jedis.pexpire()单位是毫秒数
        jedis.close();
    }

    /**
     * 设置key的存活到什么时候，单位是未来某一时间的时间戳
     * @param key
     * @param timestamp
     */
    public static void setLiveAt(String key,long timestamp){
        Jedis jedis=pool.getResource();
        jedis.expireAt(key,timestamp);//jedis.pexpireAt()单位是毫秒数
        jedis.close();
    }

    public static void publish(){
        Jedis jedis=pool.getResource();
        for (int i = 0; i <10 ; i++) {
            jedis.publish("channel1","hello kingcall");
            jedis.publish("channel2","hello kingcall   2");
        }
        jedis.close();
    }



    public static void subscribe(){
        Jedis jedis=pool.getResource();
        jedis.subscribe(new JedisPubSub() {

            @Override
            public void onMessage(String channel, String message) {
                super.onMessage(channel, message);
                System.out.println("从"+channel+" 收到的消息是："+message);
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                System.out.println("subscribe channel : " + channel + ", total channel num : " + subscribedChannels);
            }
        },"channel1","channel2");
        jedis.close();
    }

    /**
     * 普通的数据插入方式，不使用管道
     */
    public static void testInsert() {
        long currentTimeMillis = System.currentTimeMillis();
        Jedis jedis = pool.getResource();
        for (int i = 0; i < 100000; i++) {
            jedis.set("test" + i, "test" + i);
        }
        long endTimeMillis = System.currentTimeMillis();
        System.out.println(endTimeMillis - currentTimeMillis);
    }

    /**
     * 作为一款高性能工具 尤其是在批量插入的时候
     */
    public static void piplineInsert(){
        long currentTimeMillis = System.currentTimeMillis();
        Jedis jedis =pool.getResource();
        Pipeline pipelined = jedis.pipelined();
        for (int i = 0; i < 100000; i++) {
            pipelined.set("bb" + i, i + "bb");
        }
        pipelined.sync();
        long endTimeMillis = System.currentTimeMillis();
        System.out.println(endTimeMillis - currentTimeMillis);
    }
}
