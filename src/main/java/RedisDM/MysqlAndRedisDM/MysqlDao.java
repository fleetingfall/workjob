package RedisDM.MysqlAndRedisDM;

import com.alibaba.druid.pool.DruidPooledConnection;
import util.DbPoolConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 提供与数据库交互的功能
 */

public class MysqlDao {
    static DbPoolConnection pool = DbPoolConnection.getInstance();

    public static stu getResult(String name) throws SQLException {
        DruidPooledConnection connection = pool.getConnection();
        String sql="select * from Redis where name=?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1,name);
        ResultSet result = statement.executeQuery();
        return getStus(result);
    }

    public static stu getStus(ResultSet resultSet) throws SQLException {
        List<stu> list = new ArrayList<>();
        while (resultSet.next()) {
            String name = resultSet.getString("name");
            String age = resultSet.getString("age");
            String sex = resultSet.getString("sex");
            String address = resultSet.getString("address");
            list.add(new stu(name, age, sex, address));
        }
        if (list.size() == 0) {
            return null;
        } else {
            return list.get(0);
        }
    }

    public static void insert(stu stu) throws SQLException {
        DruidPooledConnection connection = pool.getConnection();
        PreparedStatement statement = connection.prepareStatement("INSERT INTO Redis(name,age,sex,address) VALUES(?,?,?,?)");
        String name=stu.getName();
        String age=stu.getAge();
        String sex=stu.getSex();
        String address=stu.getAddress();
        statement.setString(1,name);
        statement.setString(2,age);
        statement.setString(3,sex);
        statement.setString(4,address);
        System.out.println("受影响的行数："+statement.executeUpdate());
    }
}