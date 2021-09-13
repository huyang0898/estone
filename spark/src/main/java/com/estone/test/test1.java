package com.estone.test;

public class test1 {
    public static void main(String[] args) {
        String str1 = new String("");
        for(int i = 0; i < 20; i++) {
            str1 += ("hello" + i);
        }
        System.out.println(str1);
    }
}
