package RedisDM.MysqlAndRedisDM;

import com.alibaba.fastjson.JSON;
import util.JedisDM;
import java.sql.SQLException;
import java.util.Map;

/**
 * 演示Redis和Mysql结合的简单例子
 */
public class RedisMysql {
    public static void main(String[] args) throws SQLException {
        query(new stu("尧文丽","19","f","湖北"));

    }

    /**
     * 查询  首先在Redis查询 如果查询不到再去Mysql 里面查询  如果还是查询不到就将待查询数据同时加入Mysql 和Redis 里面
     */
    public static void query(stu st) throws SQLException {
        String tmpname=st.getName();
        stu tmp=getStufromRedis(tmpname);
        if (tmp!=null){
            System.out.println("Redis 命中");
            JedisDM.setExpire(st.getName(),50);
            System.out.println(tmp);
        }else{
            stu tmpstu= MysqlDao.getResult(tmpname);
            if (tmpstu==null){
                System.out.println("数据库没有命中，同时添加到数据库和Redis");
                String json=JSON.toJSONString(st);
                Map<String,String> map=JSON.parseObject(json,Map.class);
                JedisDM.setHash(st.getName(),map);
                JedisDM.setExpire(st.getName(),50);
                MysqlDao.insert(st);
            }else {
                System.out.println("数据库命中,添加到Redis");
                JedisDM.setHash(st.getName(),JSON.parseObject(JSON.toJSONString(st),Map.class));
                JedisDM.setExpire(st.getName(),50);
                System.out.println(tmpstu);
            }
        }
    }

    public static stu getStufromRedis(String name){
        Map<String,String> map=JedisDM.getHash(name);
        if (map.size()!=4||map.get("name")==null){
            return null;
        }else{
         return new stu(map);
        }
    }

}
