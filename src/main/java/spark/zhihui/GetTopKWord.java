/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.zhihui;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class GetTopKWord {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

         SparkConf sparkConf = new SparkConf().setAppName("getTopKWord");
//        SparkConf sparkConf = new
//                SparkConf().setAppName("getTopKWord").setMaster("local[4]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        
        
        JavaRDD<String> lines = ctx.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -6728992404947992526L;

            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 5059356846208028412L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).cache();

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -1120524426603033957L;

            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        }).cache();

//        for (Tuple2<?, ?> tuple : counts.collect()) {
//            System.out.println(tuple._1() + " : " + tuple._2());
//            // logger.info("====================" + tuple._1() + " : " +
//            // tuple._2());
//        }

        // counts.saveAsTextFile(args[1]);
        // counts.saveAsHadoopFile(args[1], Text.class, IntWritable.class,
        // TextOutputFormat.class);

//        List<Tuple2<Integer, String>> tmp2 = tmp3.collect();
//        for (Tuple2<Integer, String> tuple2 : tmp2) {
//            System.out.println("+++++++++++++++++++++++" + tuple2._2() + " : " + tuple2._1());
//        }

        List<Tuple2<Integer, String>> result = counts
                .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

                    private static final long serialVersionUID = 6722372394958500130L;

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuble) throws Exception {
                        return new Tuple2<Integer, String>(tuble._2(), tuble._1());// exchange
                                                                                   // key
                                                                                   // value
                    }
                }).sortByKey(true, 1).top(Integer.parseInt(args[1]), new MyComparator());

        System.out.println("top " + args[1]);
        for (Tuple2<Integer, String> tuple2 : result) {
            System.out.println("=====================" + tuple2._2() + " : " + tuple2._1());
        }

        ctx.stop();
        
    }
    
    private static class MyComparator implements Comparator<Tuple2<Integer, String>>,Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
            return o1._1().compareTo(o2._1());
        }

        
    }
}
