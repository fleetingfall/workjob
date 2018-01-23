package scala.javascala;

public class combineTrait implements traitFace {
    public static String name="kingcall";
    public String delete(String name) {
        return name+":已经被删除";
    }
    public String add(String name) {
        return name+":已经被添加";
    }

    public static void main(String[] args) {
        combineTrait combine=new combineTrait();
        System.out.println(combine.delete(name));
        System.out.println(combine.add(combineTrait.name));
    }
}
