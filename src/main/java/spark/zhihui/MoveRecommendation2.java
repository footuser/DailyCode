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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.netlib.util.doubleW;
import org.netlib.util.intW;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Tuple2;
import tachyon.thrift.WorkerService.Processor.returnSpace;
import util.zhihui.FileUtil;

/**
 * Example using MLLib ALS from Java.
 */
public final class MoveRecommendation2 {
    private static final Pattern SEPARATOR = Pattern.compile("::");

    static class ParseMovie implements PairFunction<String, Integer, String> {

        private static final long serialVersionUID = 1L;

        // input--MovieID::Title::Genres
        @Override
        public Tuple2<Integer, String> call(String input) throws Exception {
            String[] tok = SEPARATOR.split(input);
            Integer movieId = Integer.parseInt(tok[0]);
            String title = tok[1];

            return new Tuple2<Integer, String>(movieId, title);
        }

    }

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

    static class ParseRating2 implements PairFunction<String, Long, Rating> {
        private static final long serialVersionUID = 1L;

        // input format--UserID::MovieID::Rating::Timestamp
        @Override
        public Tuple2<Long, Rating> call(String input) throws Exception {
            String[] tok = SEPARATOR.split(input);
            int uid = Integer.parseInt(tok[0]);
            int movieId = Integer.parseInt(tok[1]);
            double rating = Double.parseDouble(tok[2]);
            long time = Long.parseLong(tok[3]) % 10;
            return new Tuple2<Long, Rating>(time, new Rating(uid, movieId, rating));
        }

    }

    static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String call(Tuple2<Object, double[]> element) {
            return element._1() + "," + Arrays.toString(element._2());
        }
    }

    static class getPartOfRating implements Function<Tuple2<Long, Rating>, Integer> {
        private static final long serialVersionUID = 1L;
        private String part;

        public getPartOfRating(String part) {
            this.part = part;
        }

        @Override
        public Integer call(Tuple2<Long, Rating> input) throws Exception {
            if (part.equals("user")) {
                return input._2.user();
            }
            if (part.equals("movie")) {
                return input._2.product();
            }
            return null;
        }

    }

    static class getMovieIdAndRatingCounts implements PairFunction<Tuple2<Long, Rating>, Integer, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> call(Tuple2<Long, Rating> in) throws Exception {
            return new Tuple2<Integer, Long>(in._2.product(), 1l);
        }

    }

    static class reverseKeyAndValue<k, v> implements PairFunction<Tuple2<k, v>, v, k> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<v, k> call(Tuple2<k, v> in) throws Exception {
            return new Tuple2<v, k>(in._2, in._1);
        }
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MoveRecommendation").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String inputDir = "E:/machineLearning_datas";// args[0];
        JavaRDD<String> originalRatings = sc.textFile(inputDir + "/ratings2.dat");
        JavaPairRDD<Long, Rating> ratings = originalRatings.mapToPair(new ParseRating2()).cache();
        // JavaRDD<Rating> ratings = originalRatings.map(new
        // ParseRating()).cache();

        JavaRDD<String> originalMovies = sc.textFile(inputDir + "/movies.dat");
        final Map<Integer, String> movies = originalMovies.mapToPair(new ParseMovie()).collectAsMap();

        long numRating = ratings.count();
        long numUser = ratings.map(new getPartOfRating("user")).distinct().count();
        long numMovie = ratings.map(new getPartOfRating("movie")).distinct().count();
        System.out.println("got " + numRating + " ratings from " + numUser + " users on " + numMovie + " movies.");


        int numPartitions = 20;
        JavaRDD<Rating> training = ratings.filter(new Function<Tuple2<Long, Rating>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<Long, Rating> in) throws Exception {
                return in._1 < 6;
            }
        }).values().repartition(numPartitions).cache();
        JavaRDD<Rating> validation = ratings.filter(new Function<Tuple2<Long, Rating>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<Long, Rating> in) throws Exception {
                return in._1 >= 6 && in._1 < 8;
            }
        }).values().repartition(numPartitions).cache();
        JavaRDD<Rating> test = ratings.filter(new Function<Tuple2<Long, Rating>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<Long, Rating> in) throws Exception {
                return in._1 >= 8;
            }
        }).values().repartition(numPartitions).cache();
        long numTraining = training.count();
        long numValidation = validation.count();
        long numTest = test.count();
        System.out.println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest);
        FileUtil.writeToFile("e:/result.txt",
                "Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest + "\r\n");

        int bestRank = 0;
        double bestLambda = -1.0;
        int bestNumIter = -1;
        int rank = 8;
        double lambda = 0.1;
        int numIter = 20;
        // Build the recommendation model using ALS
        MatrixFactorizationModel bestModel = ALS.train(training.rdd(), rank, numIter, lambda);

        double validationRmse = computeRmse(bestModel, validation);

        System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
        FileUtil.writeToFile("e:/result.txt",
                "RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + "." + "\r\n");

        double testRmse = computeRmse(bestModel, test);
        System.out.println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
                + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".");
        FileUtil.writeToFile("e:/result.txt",
                "The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
                        + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + "."
                        + "\r\n");

        RDD<Rating> predictDatas = bestModel.predict(test.map(new Function<Rating, Tuple2<Object, Object>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Object, Object> call(Rating in) throws Exception {
                return new Tuple2<Object, Object>(in.user(), in.product());
            }
        }).rdd());
        predictDatas.saveAsTextFile("e:/result2");
        
        
        for (Rating rating : bestModel.recommendProducts(8, 20)) {
            System.out.println("recommend for you: " + movies.get(rating.product()));
            FileUtil.writeToFile("e:/result.txt",
                    "recommend for you: " + movies.get(rating.product()) + "\r\n");
        }

        sc.stop();
    }

    private static double computeRmse(MatrixFactorizationModel model, JavaRDD<Rating> training) {
        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = training.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    private static final long serialVersionUID = 1L;

                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
                );
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            private static final long serialVersionUID = 1L;

                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                        ));
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(training.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            private static final long serialVersionUID = 1L;

                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                        )).join(predictions).values();
        double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    private static final long serialVersionUID = 1L;

                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
                ).rdd()).mean();
        return MSE;
    }

    private static String readUserInput(String prompt) {
        // 先定义接受用户输入的变量
        String result = null;
        do {
            // 输出提示文字
            System.out.print(prompt);
            InputStreamReader is_reader = new InputStreamReader(System.in);
            try {
                result = new BufferedReader(is_reader).readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (isInvalid(result)); // 当用户输入无效的时候，反复提示要求用户输入
        return result;
    }

    private static boolean isInvalid(String in) {
        try {
            double rating = Double.parseDouble(in);
            if (rating >= 0 && rating <= 5) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
