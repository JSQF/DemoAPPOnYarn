package com.yyb.learn;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2020-07-20
 * @Time 09:21
 */
public class Executor {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Executor");
        System.out.println("Executor Start ... ");
        for(int i=0;i<30;i++){
            System.out.println("running ...");
            Thread.sleep(2000);
        }
        System.out.println("Executor End ... ");
        System.exit(0);
    }
}
