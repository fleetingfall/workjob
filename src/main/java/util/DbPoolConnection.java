package util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

public class DbPoolConnection {
    private static DbPoolConnection databasePool=null;
    private static DruidDataSource dds = null;
    static {
        Properties properties = loadPropertyFile("DB.properties");
        try {
            dds = (DruidDataSource) DruidDataSourceFactory
                    .createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private DbPoolConnection() {}
    public static synchronized DbPoolConnection getInstance() {
        if (null == databasePool) {
            databasePool = new DbPoolConnection();
        }
        return databasePool;
    }
    public  DruidPooledConnection getConnection() throws SQLException {
        return dds.getConnection();
    }
    public static Properties loadPropertyFile(String fullFile) {
        Properties p = new Properties();
        if(fullFile == "" || fullFile.equals(""))
        {
            System.out.println("属性文件为空!~");
        }
        else
        {
            InputStream inStream = DbPoolConnection.class.getClassLoader().getResourceAsStream(fullFile);
            try {
                p.load(inStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return p;
    }

}
