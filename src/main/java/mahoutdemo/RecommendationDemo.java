package mahoutdemo;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import org.apache.mahout.cf.taste.similarity.UserSimilarity;


import java.io.File;
import java.util.List;

public class RecommendationDemo {

    private RecommendationDemo() {

    }

    public static void main(String[] args) throws Exception{

        /**
         * user_based cf模型示例
         */
        DataModel model = new FileDataModel(new File("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\movie_recommender-master\\ml-1m\\ratings.csv"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(20,  similarity, model);
        Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
        // csv格式, 必须是原始格式
        List<RecommendedItem> recommendedItems = recommender.recommend(2,20);

        for (RecommendedItem recommendedItem : recommendedItems) {
            System.out.println(recommendedItem);

        }
    }
}
