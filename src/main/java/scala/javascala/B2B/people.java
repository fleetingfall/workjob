package scala.javascala.B2B;

/**
 * @Authork:kingcall
 * @Description:
 * @Date:$time$ $date$
 */
public class people {

    String name;
    int age;

    public people() {
    }

    public people(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "people{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
