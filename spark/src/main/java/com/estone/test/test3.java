package com.estone.test;

public class test3 {
    public static void main(String[] args) {
        String name = "john";
        int age = 10;
        double score = 98.3 / 3;
        char gender = '男';
        // String info =
        // "我的姓名是"+name+"年龄是"+age+",成绩是"+score+"性别是"+gender+"。希望大家喜欢我！";
        String info = String
                .format("我的姓名是%s 年龄是%d，成绩是%.2f 性别是%c. 希望大家喜欢我！",name, age, score, gender);
        System.out.println(info);
    }
}
