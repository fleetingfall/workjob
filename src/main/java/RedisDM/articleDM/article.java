package RedisDM.articleDM;

import org.mortbay.util.ajax.JSON;
import redis.clients.jedis.Jedis;
import util.JedisDM;

import java.util.ArrayList;
import java.util.Set;

public class article {
    static int WEEKSECONDS=7*86400;
    static int VOTESCORE=432;
    //存储文章分数的有序集合
    static final String articleScoreZset="ScoreZ";
    //存储文章发布时间的有序集合
    static final String articleTimeZset="TimeZ";
    //文章的一些属性
    static final String TITLE="title";
    static final String LINK="link";
    static final String POSTER="poster";
    static final String TIME="time";
    static final String VOTES="votes";
    static final String GROUP="group";//文章的分组信息
    //其他属性
    static final int PAGESIZE=20;
    static final String SORT="sort:";

    /**
     * 用户给文章投票的函数
     * @param userID        user:2020
     * @param articleID  article:8080
     */
    public static boolean vote(String userID,String articleID){
        if (timeCheck(articleID)){
            String[] tmp=articleID.split(":");
            String voteID="vote:"+tmp[1];
            if (adduser(userID,voteID)>0){
                try {
                    JedisDM.zIncreBy(articleScoreZset,VOTESCORE,articleID);
                    JedisDM.HashIncreBy(articleID,VOTES,1);
                    System.out.println("投票成功");
                    return true;
                }catch (Exception e){
                    System.out.println("投票失败");
                    return false;
                }
            }else {
                System.out.println("不能重复投票");
                return false;
            }
        }else {
            System.out.println("投票时间已经截止");
            return false;
        }
    }

    /**
     * 将用户添加到具体的文章用户集合
     * @param userID
     * @param voteID    vote:2020
     */
    public static long adduser(String userID,String voteID){
       return JedisDM.sadd(voteID,userID);
    }

    /**
     * 发布文章的函数
     * @param articleID
     * @param title
     * @param link
     * @param poster
     */
    public static void publishArticle(String articleID,String title,String link,String poster){
        if (JedisDM.getHash(articleID).isEmpty()) {
            String time = Long.toString(System.currentTimeMillis()).substring(0,10);
            String votes = "0";
            JedisDM.setHash(articleID, TITLE, title);
            JedisDM.setHash(articleID, LINK, link);
            JedisDM.setHash(articleID, POSTER, poster);
            JedisDM.setHash(articleID, TIME, time);
            JedisDM.setHash(articleID, VOTES, votes);
            //将文章添加到分数集合和时间集合
            JedisDM.zadd(articleScoreZset,0,articleID);
            JedisDM.zadd(articleTimeZset,Double.parseDouble(time),articleID);
        }else{
            System.out.println("文章ID重复，请重新发布");
        }
    }

    /**
     *
     * @param articleID
     * @param title
     * @param link
     * @param poster
     * @param group  group:programing
     */
    public static void publishArticle(String articleID,String title,String link,String poster,String ... group){
        if (JedisDM.getHash(articleID).isEmpty()) {
            String time = Long.toString(System.currentTimeMillis()).substring(0,10);
            String votes = "0";
            JedisDM.setHash(articleID, TITLE, title);
            JedisDM.setHash(articleID, LINK, link);
            JedisDM.setHash(articleID, POSTER, poster);
            JedisDM.setHash(articleID, TIME, time);
            JedisDM.setHash(articleID, VOTES, votes);
            String tmpGroup="";
            for (String g:group){
                tmpGroup+=","+g;
            }
            JedisDM.setHash(articleID,GROUP,tmpGroup);
            setGroup(articleID,group);
            //将文章添加到分数集合和时间集合
            JedisDM.zadd(articleScoreZset,0,articleID);
            JedisDM.zadd(articleTimeZset,Double.parseDouble(time),articleID);
        }else{
            System.out.println("文章ID重复，请重新发布");
        }
    }

    /**
     * 将文章添加到对应的分组中
     * @param articleID
     * @param group
     */
    public static void setGroup(String articleID,String ... group){
        for (String g:group){
            JedisDM.sadd(g,articleID);
        }
        String tmpGroup="";
        for (String g:group){
            tmpGroup+=","+g;
        }
        JedisDM.setHash(articleID,GROUP,tmpGroup);
    }

    /**
     * 检查文章的发布时间是否超过一周
     * @param articleID
     * @return
     */
    public static boolean timeCheck(String articleID){
        double pubTime=JedisDM.zgetScore(articleTimeZset,articleID);
        if (System.currentTimeMillis()/1000-pubTime<=WEEKSECONDS)
            return true;
        else
            return false;
    }

    /**
     * 根据文章的评分获取文章的全部信息
     * @return
     */
    public static  ArrayList<String> getArticles(){
        ArrayList<String> info=new ArrayList<>(1000);
        Jedis jedis=JedisDM.pool.getResource();
        Set<String> ids=jedis.zrevrange(articleScoreZset,0,-1);
        for(String id:ids){
            info.add(JSON.toString(JedisDM.getHash(id)));
        }
        return info;
    }

    /**
     * 获取第多少页的文章信息
     * @param articleID
     * @param page
     * @return
     */
    public static  ArrayList<String> getArticles(String articleID,int page){
        ArrayList<String> info=new ArrayList<>(PAGESIZE);
        Jedis jedis=JedisDM.pool.getResource();
        int start=(page-1)*PAGESIZE;
        int end= start+PAGESIZE-1;
        Set<String> ids=jedis.zrevrange(articleScoreZset,start,end);
        for(String id:ids){
            info.add(JSON.toString(JedisDM.getHash(id)));
        }
        return info;
    }

    /**
     * 获取某一分组里的文章信息
     * @param group   program
     * @return
     */
    public static  ArrayList<String> getArticlesByGroup(String group){
        ArrayList<String> info=new ArrayList<>(PAGESIZE);
        Jedis jedis=JedisDM.pool.getResource();
        String subgroup=group.split(":")[1];
        jedis.zinterstore(SORT+subgroup,group,articleScoreZset);
        Set<String> ids=jedis.zrevrange(SORT+subgroup,0,-1);
        for(String id:ids){
            info.add(id+"\t"+JSON.toString(JedisDM.getHash(id)));
        }
        return info;
    }




}
