package com;

import com.control.Recommend;
import scala.collection.mutable.WrappedArray;

import java.util.Arrays;


public class Test {
    public  int[] randomCommon(int min, int max, int n){
        if (n > (max - min + 1) || max < min) {
            return null;
        }
        int[] result = new int[n];
        int count = 0;
        while(count < n) {
            int num = (int) (Math.random() * (max - min)) + min;
            boolean flag = true;
            for (int j = 0; j < n; j++) {
                if(num == result[j]){
                    flag = false;
                    break;
                }
            }
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }

    public  int[] randomCommonDu(int min, int max, int n){
        int[] result = new int[n];
        int count = 0;
        while(count < n) {
            int num = (int) (Math.random() * (max - min)) + min;
            boolean flag = true;
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }
    public static void main(String []arg){
            Test test=new Test();
            for(int j=1;j<97;j++) {
                int[] itemid=test.randomCommon(1,50,20);
                int[] count=test.randomCommonDu(1,30,20);
                for (int i = 0; i < 20; i++) {
                    System.out.println(j+"::" + itemid[i] + "::" + count[i] + "::" + "977594172");
                }
            }
    }

}
