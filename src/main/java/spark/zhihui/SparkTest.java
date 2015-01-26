package spark.zhihui;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SparkTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test-spark").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        
        //通过Scala集合对象并行化生成
        JavaRDD<Integer> originalDatas = sc.parallelize(data);

        //start
        JavaRDD<Integer> afterMapDatas = originalDatas.map(new Function<Integer, Integer>() {

            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer in) throws Exception {
                return in;
            }
        });
        
//        afterMapDatas.persist(StorageLevel.DISK_ONLY());

        Integer result = afterMapDatas.reduce(new Function2<Integer, Integer, Integer>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        System.out.println(result);
        
        sc.stop();
        

    }

}
