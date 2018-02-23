package constant;

import redis.clients.jedis.JedisPoolConfig;

public class JedisPooLConf {
    static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

    public static JedisPoolConfig getJedisPoolConfig(int maxIdle,int minIdle,int maxtotal,long wait) {
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMinIdle(minIdle);
        jedisPoolConfig.setMaxTotal(maxtotal);
        jedisPoolConfig.setBlockWhenExhausted(true);//连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
        jedisPoolConfig.setMaxWaitMillis(wait);//获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        jedisPoolConfig.setTestWhileIdle(true);////在空闲时检查有效性, 默认true 可以在该对象的构造方法中可以看到
        return jedisPoolConfig;
    }
}
