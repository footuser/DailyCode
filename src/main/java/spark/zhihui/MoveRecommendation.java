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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * Example using MLLib ALS from Java.
 */
public final class MoveRecommendation {
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
        SparkConf sparkConf = new SparkConf().setAppName("MoveRecommendation");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String inputDir = args[0];
        JavaRDD<String> originalRatings = sc.textFile(inputDir + "/ratings.dat");
        JavaPairRDD<Long, Rating> ratings = originalRatings.mapToPair(new ParseRating2()).cache();
        // JavaRDD<Rating> ratings = originalRatings.map(new
        // ParseRating()).cache();

        JavaRDD<String> originalMovies = sc.textFile(inputDir + "/movies.dat");
        final Map<Integer, String> movies = originalMovies.mapToPair(new ParseMovie()).collectAsMap();

        long numRating = ratings.count();
        long numUser = ratings.map(new getPartOfRating("user")).distinct().count();
        long numMovie = ratings.map(new getPartOfRating("movie")).distinct().count();
        System.out.println("got " + numRating + " ratings from " + numUser + " users on " + numMovie + " movies.");

        // movieId,rating counts
        JavaPairRDD<Integer, Long> movieIdAndRatingCounts =
                ratings.mapToPair(new getMovieIdAndRatingCounts()).reduceByKey(new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long in0, Long in1) throws Exception {
                        return in0 + in1;
                    }
                });

        // 1.reverse key and value to rating counts,movieId
        // 2.sort by rating counts desc
        // 3.reverse to movieId,rating counts
        // 4.change to movieId,movieTitle
        // 5.take 15th
        List<Tuple2<Integer, String>> hotMovies = movieIdAndRatingCounts
                .mapToPair(new reverseKeyAndValue<Integer, Long>())
                .sortByKey(false).mapToPair(new reverseKeyAndValue<Long, Integer>())
                .map(new Function<Tuple2<Integer, Long>, Tuple2<Integer, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<Integer, Long> in) throws Exception {
                        return new Tuple2<Integer, String>(in._1, movies.get(in._1));
                    }
                }).takeOrdered(15);

        List<Tuple2<Integer, Double>> myRating = new ArrayList<Tuple2<Integer, Double>>();
        for (Tuple2<Integer, String> tuple2 : hotMovies) {
            myRating.add(new Tuple2<Integer, Double>(tuple2._1, Double.parseDouble(readUserInput("请输入对电影：" + tuple2._2
                    + "  的评分："))));
        }
        // get my rating
        JavaRDD<Rating> myRatingRdd = sc.parallelize(myRating).map(new Function<Tuple2<Integer, Double>, Rating>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Rating call(Tuple2<Integer, Double> in) throws Exception {
                return new Rating(0, in._1, in._2);
            }
        });

        // JavaPairRDD<Long, Rating> myRatingRdd =
        // sc.parallelize(myRating).mapToPair(
        // new PairFunction<Tuple2<Integer,Double>, Long, Rating>() {
        // private static final long serialVersionUID = 1L;
        //
        // @Override
        // public Tuple2<Long, Rating> call(Tuple2<Integer,Double> in) throws
        // Exception {
        // return new Tuple2<Long, Rating>(System.currentTimeMillis() % 10, new
        // Rating(0, in._1, in._2));
        // }
        // });

        int numPartitions = 20;
        JavaRDD<Rating> training = ratings.filter(new Function<Tuple2<Long, Rating>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<Long, Rating> in) throws Exception {
                return in._1 < 6;
            }
        }).values().union(myRatingRdd).repartition(numPartitions).cache();
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

        MatrixFactorizationModel bestModel = null;
        double bestValidationRmse = Double.MAX_VALUE;
        int bestRank = 0;
        double bestLambda = -1.0;
        int bestNumIter = -1;
        int[] ranks = {8, 12};
        double[] lambdas = {0.1, 10.0};
        int[] numIters = {10, 20};
        for (int numIter : numIters) {
            for (int rank : ranks) {
                for (double lambda : lambdas) {
                    // Build the recommendation model using ALS
                    MatrixFactorizationModel model = ALS.train(training.rdd(), rank, numIter, lambda);

                    double validationRmse = computeRmse(model, validation);

                    System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                            + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
                    if (validationRmse < bestValidationRmse) {
                        bestModel = model;
                        bestValidationRmse = validationRmse;
                        bestRank = rank;
                        bestLambda = lambda;
                        bestNumIter = numIter;
                    }
                }
            }
        }
        
        double testRmse = computeRmse(bestModel, test);
        System.out.println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
                + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".");

        
//        List<Integer> myRatingMovieIds = new ArrayList<Integer>();
//        for (Tuple2<Integer, Double> tuple2 : myRating) {
//            myRatingMovieIds.add(tuple2._1);
//        }
        for (Rating rating : bestModel.recommendProducts(0, 20)) {
            System.out.println("recommend for you: " + movies.get(rating.product()));
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
            Integer rating = Integer.parseInt(in);
            if (rating >= 0 && rating <= 5) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
