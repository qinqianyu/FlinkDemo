package cn.jxk.source;

import lombok.Data;

@Data
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;

    public Student() {
    }

    public Student(int id, String name, String password, int age) {
        this.id = id;
        this.name = name;
        this.password = password;
        this.age = age;
    }

}

