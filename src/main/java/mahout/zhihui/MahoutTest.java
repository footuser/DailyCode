package mahout.zhihui;

import java.io.File;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class MahoutTest {

    public static void main(String[] args) throws Exception {
//        userCfTest();
        itemCfTest();
    }

    private static void userCfTest() throws Exception {
        DataModel model = new FileDataModel(new File("e:/u1.test"));
        UserSimilarity user = new PearsonCorrelationSimilarity(model);
        NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(3, user, model);
        Recommender r = new GenericUserBasedRecommender(model, neighbor, user);
        LongPrimitiveIterator iter = model.getUserIDs();

        while (iter.hasNext()) {
            long uid = iter.nextLong();
            List<RecommendedItem> list = r.recommend(uid, 5);
            System.out.printf("uid:%s", uid);
            for (RecommendedItem ritem : list) {
                System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
            }
            System.out.println();
        }
    }

    private static void itemCfTest() throws Exception {
        DataModel model = new FileDataModel(new File("e:/u1.test"));// 构造数据模型，File-based
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
