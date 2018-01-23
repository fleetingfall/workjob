package scala.scalajava;

public class Person {
    public static void say(){
        System.out.println("测试方法，测试在scala中直接调用java代码");
    }
    public String name;
    public int age;

    public Person(String name,int age) {
        this.name = name;
        this.age = age;
    }
    public void getMessage(){
        System.out.println("姓名:"+name+",年龄:"+age);
    }
}
