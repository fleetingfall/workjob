package util;

import kafka.utils.VerifiableProperties;
import kafka.utils.ZKConfig;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * 是ZKUtils的java版本
 * ZkClient是由Datameer的工程师开发的开源客户端，对Zookeeper的原生API进行了包装，实现了超时重连、Watcher反复注册等功能。本身功能就很强大 是用java写的
 * kafka本身就有一套关于zk的工具类 kafka.utils.{VerifiableProperties, ZKConfig, ZKGroupTopicDirs, ZkUtils}  这是用scala 的
 * 问题是 zkclient zookeeper(也可以直接创建zookeeper对象来进行操作) 和 zkutils之间的关系
 *         kafak中的zkutil是为了将kafak的offset保存到zookeeper上去，所以它借助了zkclient
 *         而zkclient是借助可zookeeper的api的
 *
 */
class User implements Serializable{
    int id;
    String name;

    public User(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}

public class ZKClientDM {

    public static void main(String[] args) {
        updateNode();
    }
    public static ZkClient zkClient=null;
    static String zkQuorum = "master:2181";
    public static ZkClient getZKClient(String zkQuorum){
      if (zkClient==null){
          Properties props = new Properties();
          props.put("zookeeper.connect", zkQuorum);
          props.put("zookeeper.connection.timeout.ms", "30000");
          ZKConfig zkConfig = new ZKConfig(new VerifiableProperties(props));
          zkClient=new ZkClient(zkConfig.zkConnect(), zkConfig.zkSessionTimeoutMs(), zkConfig.zkConnectionTimeoutMs(),new SerializableSerializer());
          return zkClient;
      }else {
          return zkClient;
      }
   }

    /**
     * 创建节点 路径不支持递归创建
     * @return
     */
   public static String CreateNode(){
        ZkClient zkClient=getZKClient(zkQuorum);
        User user=new User(1,"刘文强");
        String path=zkClient.create("/king2",user, CreateMode.PERSISTENT);
        System.out.println(path);
        return path;
   }

    /**
     *
     */
   public static void getData(){
       ZkClient zkClient=getZKClient(zkQuorum);
       Stat stat = new Stat();
       //获取 节点中的对象
       zkClient.readData("/king2",stat);
       System.out.println(stat);
   }

    /**
     *
     * @return
     */
   public static boolean existsPath(){
       ZkClient zkClient=getZKClient(zkQuorum);
       boolean e = zkClient.exists("/king2/pd");
       if (e){
           System.out.println("节点存在");
       }else {
           System.out.println("节点不存在");
       }
       return e;
   }

    /**
     * 删除节点的方法
     * @param Recursive     是否采取递归删除
     * @return
     */
   public static boolean deleteNode(boolean Recursive){
       ZkClient zkClient=getZKClient(zkQuorum);
       boolean e1=false;
       if (Recursive==true){
           e1 = zkClient.deleteRecursive("/king2");
           System.out.println("删除成功，采取的是递归删除的方法");
       }else {
          try {
              e1 = zkClient.delete("/king2");
          }catch (Exception e){
              System.out.println("删除失败：节点不为空——————该节点下还有子节点，请采用递归删除");
          }
       }
       return e1;
   }

    /**
     * 有问题
     */

   public static void updateNode(){
       User user = new User(2,"kingcall");
       zkClient.writeData("/king2", "fsdfdsf");
       System.out.println("更新数据节点成功");
   }




}
