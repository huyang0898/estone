package com.estone.test;

public class test2 {
    public static void main(String[] args) {
        //判断邮箱是否合法，要求里面必须包含@和.    而且 @ 必须在. 的前面
        //huyang@126.com
        //不使用系统提供的trim 方法，自己写一个myTrim方法，去除字符串两端的空格
        String s =  "     hello world   ";
//       int a = emailName.indexOf("@");
//       int b = emailName.indexOf(".");
//        if(a != -1 && b != -1 && a < b){
//            System.out.println("邮箱合法");
//        }else {
//            System.out.println("邮箱不合法");
//        }


        int a = 0;
        int b = 0;

        for(int i = 0; i <= s.length()-1 ; i++){
          if(s.charAt(i) != ' '){
             // System.out.println(i);
              a = i;
              break;
          }

          }

        for (int i = s.length() - 1;  i >= 0; i--){
            if(s.charAt(i) != ' '){
                //System.out.println(i);
                b = i + 1;
                break;
            }
        }
        //System.out.println(a);
        System.out.println(s.substring(a, b));

    }



    }

