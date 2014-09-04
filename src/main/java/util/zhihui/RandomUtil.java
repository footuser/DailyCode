/*
 * Copyright (c) 2012 Sohu. All Rights Reserved
 */
package util.zhihui;

/**
 * <p>
 * Description:
 * </p>
 * 
 * @author jiannanfan
 * @version 1.0
 * @Date 2014-2-20
 */
public class RandomUtil {
    public static int next2() {
        return getRandom(10) + 10;
    }

    public static int getRandom(int seed) {
        return (int) (Math.random() * seed);
    }
    
    public static long getRandom2(long seed) {
        return (long) (Math.random() * seed);
    }

    public static void main(String[] args) {
        System.out.println(next2());
    }
}
