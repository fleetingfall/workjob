package util;

import kafka.utils.VerifiableProperties;
import kafka.utils.ZKConfig;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * 是ZKUtils的java版本
 * ZkClient是由Datameer的工程师开发的开源客户端，对Zookeeper的原生API进行了包装，实现了超时重连、Watcher反复注册等功能。本身功能就很强大 是用java写的
 * kafka本身就有一套关于zk的工具类 kafka.utils.{VerifiableProperties, ZKConfig, ZKGroupTopicDirs, ZkUtils}  这是用scala 的
 * 问题是 zkclient zookeeper(也可以直接创建zookeeper对象来进行操作) 和 zkutils之间的关系
 *         kafak中的zkutil是为了将kafak的offset保存到zookeeper上去，所以它借助了zkclient
 *
 */
public class ZKClientDM {
   public static ZkClient getZKClient(String zkQuorum){
       Properties props = new Properties();
       props.put("zookeeper.connect", zkQuorum);
       props.put("zookeeper.connection.timeout.ms", "30000");
       ZKConfig zkConfig = new ZKConfig(new VerifiableProperties(props));
       ZkClient zkClient=new ZkClient(zkConfig.zkConnect(), zkConfig.zkSessionTimeoutMs(), zkConfig.zkConnectionTimeoutMs());
       return zkClient;
   }



}
