/*
 * Copyright (c) 2012 Sohu. All Rights Reserved
 */
package util.zhihui;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import model.zhihui.SucaiVideoSimple;

/**
 * 随机排序算法
 * 
 * @author zhihuiqiu
 */
public class RandomSort {

    public static Comparator<SucaiVideoSimple> comparator = new Comparator<SucaiVideoSimple>() {

        @Override
        public int compare(SucaiVideoSimple o1, SucaiVideoSimple o2) {
            if (o1.getWeight() == o2.getWeight()) {
                return RandomUtil.getRandom(2) == 0 ? 1 : -1;
            }
            int total = o1.getWeight() + o2.getWeight();
            int temp = RandomUtil.getRandom(total);
            int r = 0;
            if (o1.getWeight() > o2.getWeight()) {
                r = temp < o2.getWeight() ? 1 : -1;
            } else {
                r = temp < o1.getWeight() ? -1 : 1;
            }
            return r;
        }
    };

    public static void sort(List<SucaiVideoSimple> list) {
        Collections.sort(list, comparator);
    }

    /**
     * @author zhihuiqiu
     * @param args 测试上面排序算法
     */
    public static void main(String[] args) {
        SucaiVideoSimple o1 = new SucaiVideoSimple();
        o1.setWeight(10);
        o1.setTitle("o1");
        SucaiVideoSimple o2 = new SucaiVideoSimple();
        o2.setWeight(5);
        o2.setTitle("o2");
        List<SucaiVideoSimple> list = new ArrayList<SucaiVideoSimple>();
        list.add(o1);
        list.add(o2);
        int i = 0;
        int oo2 = 0;
        int oo1 = 0;
        while (i++ < 100000) {
            RandomSort.sort(list);
            if (list.get(0).getTitle().equals("o1")) {
                oo1++;
            }
            if (list.get(0).getTitle().equals("o2")) {
                oo2++;
            }
        }
        System.out.println("o1:" + oo1);
        System.out.println("o2:" + oo2);
    }

}
