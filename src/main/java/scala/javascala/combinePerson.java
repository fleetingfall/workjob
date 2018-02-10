package scala.javascala;

public class combinePerson {
    public static void main(String[] args) {
        Person person=new Person("kingcall",25);
        System.out.println(person.getMesaage());
        System.out.println("对应的get方法被调用"+person.name());
        person.age_$eq(29);
        System.out.println("对应的set方法被调用"+person.age());
        System.out.println("BeanProperty注解让其有了 java形式的get set方法"+person.getName());

    }
}
