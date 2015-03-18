package spark.zhihui;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;


public class GetHotRating {
    private static final Pattern SEPARATOR = Pattern.compile("::");
    static class ParseRating implements Function<String, Rating> {

        private static final long serialVersionUID = 1L;

        // input format--UserID::MovieID::Rating::Timestamp
        @Override
        public Rating call(String input) {
            String[] tok = SEPARATOR.split(input);
            int uid = Integer.parseInt(tok[0]);
            int movieId = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);
            return new Rating(uid, movieId, rating);
        }

    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf().setAppName("Test-spark-speed!");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String inputDir = args[0];
        JavaRDD<String> originalRatings = sc.textFile(inputDir + "/ratings.dat");
        JavaRDD<Rating> ratings = originalRatings.map(new ParseRating()).cache();


        JavaRDD<Rating> hots = ratings.filter(new Function<Rating, Boolean>() {
            @Override
            public Boolean call(Rating in) throws Exception {
                return in.rating() > 3.0;
            }
        }).cache();

        System.out.println("rating > 3 count: " + hots.count());
        System.out.println("spark cost:" + (System.currentTimeMillis() - time) + "ms");

        sc.stop();
    }
}
