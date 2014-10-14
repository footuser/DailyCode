package mahout.zhihui;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.LoadEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class MahoutTest {

    public static void main(String[] args) throws Exception {
        // userCfTest();
        // itemCfTest();
        userCfEvaluate();
        // testPrefence();
    }

    private static void test() throws Exception {
        DataModel model = new GroupLensDataModel(new File("ratings.dat"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood =
                new NearestNUserNeighborhood(100, similarity, model);
        Recommender recommender =
                new GenericUserBasedRecommender(model, neighborhood, similarity);
        LoadEvaluator.runLoad(recommender);
    }

    private static void testPrefence() {
        PreferenceArray user1Prefs = new GenericUserPreferenceArray(2);
        user1Prefs.setUserID(0, 2L);
        user1Prefs.setItemID(0, 101L);
        user1Prefs.setValue(0, 2.0f);
        user1Prefs.setItemID(1, 102L);
        user1Prefs.setValue(1, 3.0f);
        Preference pref = user1Prefs.get(1);
        System.out.println(pref.getUserID() + ":" + pref.getItemID());
    }

    private static void userCfEvaluate() throws Exception {
        // DataModel model = new GenericBooleanPrefDataModel(
        // GenericBooleanPrefDataModel.toDataMap(
        // new FileDataModel(new File("e:/mahout_example/ua.base"))));
        // RecommenderEvaluator evaluator =
        // new AverageAbsoluteDifferenceRecommenderEvaluator();
        // RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
        // public Recommender buildRecommender(DataModel model)
        // throws TasteException {
        // UserSimilarity
        // similarity =
        // new LogLikelihoodSimilarity(model);
        // UserNeighborhood neighborhood =
        // new NearestNUserNeighborhood(10, similarity, model);
        // return
        // new GenericUserBasedRecommender(model, neighborhood, similarity);
        // }
        // };
        // DataModelBuilder modelBuilder = new DataModelBuilder() {
        // public DataModel buildDataModel(
        // FastByIDMap<PreferenceArray> trainingData) {
        // return new GenericBooleanPrefDataModel(
        // GenericBooleanPrefDataModel.toDataMap(
        // trainingData));
        // }
        // };
        // double score = evaluator.evaluate(
        // recommenderBuilder, modelBuilder, model, 0.9, 1.0);
        // System.out.println(score);

        // RandomUtils.useTestSeed();
        // DataModel model = new FileDataModel(new
        // File("e:/mahout_example/ua.base"));
        //
        // RecommenderEvaluator evaluator = new
        // AverageAbsoluteDifferenceRecommenderEvaluator();
        // RecommenderBuilder builder = new RecommenderBuilder() {
        // @Override
        // public Recommender buildRecommender(DataModel model)
        // throws TasteException {
        // UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        // UserNeighborhood neighborhood =
        // new NearestNUserNeighborhood(2, similarity, model);
        // return
        // new GenericUserBasedRecommender(model, neighborhood, similarity);
        // }
        // };
        // double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
        // System.out.println(score);

        // RandomUtils.useTestSeed();
        // DataModel model = new FileDataModel(new
        // File("e:/mahout_example/intro.csv"));
        // RecommenderIRStatsEvaluator evaluator =
        // new GenericRecommenderIRStatsEvaluator();
        // RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
        // @Override
        // public Recommender buildRecommender(DataModel model)
        // throws TasteException {
        // UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        // UserNeighborhood neighborhood = new NearestNUserNeighborhood(2,
        // similarity, model);
        // return
        // new GenericUserBasedRecommender(model, neighborhood, similarity);
        // }
        // };
        // IRStatistics stats = evaluator.evaluate(
        // recommenderBuilder, null, model, null, 2,
        // GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,
        // 1.0);
        // System.out.println(stats.getPrecision());
        // System.out.println(stats.getRecall());

        DataModel model = new GenericBooleanPrefDataModel(
                new FileDataModel(new File("e:/mahout_example/ua.base")));
        RecommenderIRStatsEvaluator evaluator =
                new GenericRecommenderIRStatsEvaluator();
        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                UserSimilarity similarity = new LogLikelihoodSimilarity(model);
                UserNeighborhood neighborhood =
                        new NearestNUserNeighborhood(10, similarity, model);
                return new GenericBooleanPrefUserBasedRecommender(
                        model, neighborhood, similarity);
            }
        };
        DataModelBuilder modelBuilder = new DataModelBuilder() {
            @Override
            public DataModel buildDataModel(
                    FastByIDMap<PreferenceArray> trainingData) {
                return new GenericBooleanPrefDataModel(
                        GenericBooleanPrefDataModel.toDataMap(trainingData));
            }
        };
        IRStatistics stats = evaluator.evaluate(
                recommenderBuilder, modelBuilder, model, null, 10,
                GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,
                1.0);
        System.out.println(stats.getPrecision());
        System.out.println(stats.getRecall());
    }

    private static void userCfTest() throws Exception {
        DataModel model = new FileDataModel(new File("e:/mahout_example/ua.base"));

        UserSimilarity user = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighbor = new NearestNUserNeighborhood(2, user,
                model);
        Recommender r = new GenericUserBasedRecommender(model,
                neighbor, user);
        LongPrimitiveIterator iter = model.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            List<RecommendedItem> list = r.recommend(uid, 1);
            System.out.printf("uid:%s", uid);
            for (RecommendedItem ritem : list)
            {
                System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
            }
            System.out.println();
        }

    }

    private static void itemCfTest() throws Exception {
        DataModel model = new FileDataModel(new File("e:/mahout_example/intro.csv"));// 构造数据模型，File-based
        // DataModel model=new
        // GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(model1));
        ItemSimilarity similarity = new PearsonCorrelationSimilarity(model);// 计算内容相似度
        Recommender recommender = new GenericItemBasedRecommender(model, similarity);// 构造推荐引擎

        LongPrimitiveIterator iter = model.getUserIDs();

        while (iter.hasNext()) {
            long uid = iter.nextLong();
            List<RecommendedItem> list = recommender.recommend(uid, 3);
            System.out.printf("uid:%s", uid);
            for (RecommendedItem ritem : list) {
                System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
            }
            System.out.println();
        }
    }

}
