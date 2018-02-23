package RedisDM.MysqlAndRedisDM;

import java.util.Map;

/**
 * @Authork:kingcall
 * @Description:
 * @Date:$time$ $date$
 */
public class stu {
    String name;
    String age;
    String sex;
    String address;

    public stu() {
    }

    public stu(String name, String age, String sex, String address) {

        this.name = name;
        this.age = age;
        this.sex = sex;
        this.address = address;
    }
    public  stu( Map<String,String> map){
        this.name = map.get("name");
        this.age = map.get("age");
        this.sex = map.get("sex");
        this.address = map.get("address");
    }


    @Override
    public String toString() {
        return "name:"+name+"   age:"+age+"     sex:"+sex+"     address:"+address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
