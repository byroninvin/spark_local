package mahoutdemo.bookcrossing;

/**
 * Created by Joe.Kwan on 2018/11/5 10:26.
 */
//public class BXRecommender implements Recommender {
//    private Recommender recommender;
//    public BXRecommender(DataModel dataModel) throws TasteException {
//        UserSimilarity similarity = new EuclideanDistanceSimilarity(dataModel);
//        UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, 0.2, similarity, dataModel, 0.2);
//        recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
//    }
//
//    public List<RecommendedItem> recommend(long userID, int howMany) throws TasteException {
//        return recommender.recommend(userID, howMany, (IDRescorer) null, false);
//    }
//
//    public List<RecommendedItem> recommend(Long userID, int howMany, boolean includeKnownItems) throws TasteException {
//        return recommender.recommend(userID, howMany, (IDRescorer) null, includeKnownItems);
//    }
//
//    public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer rescorer) throws TasteException {
//        return recommender.recommend(userID, howMany, rescorer, false);
//    }
//
//    @Override
//    public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer idRescorer, boolean includeKnownItems) throws TasteException {
//        return recommender.recommend(userID, howMany, (IDRescorer) null, includeKnownItems);
//    }
//
//    @Override
//    public float estimatePreference(long userID, long itemID) throws TasteException {
//        return recommender.estimatePreference(userID, itemID);
//    }
//
//    public void setPreference(long userID, long itemID, float value) throws TasteException {
//        recommender.setPreference(userID, itemID, value);
//    }
//
//    public void removePreference(long userID, long itemID) throws TasteException {
//        recommender.removePreference(userID, itemID);
//    }
//
//    public DataModel getDataModel() {
//        return recommender.getDataModel();
//    }
//
//    @Override
//    public void refresh(Collection<Refreshable> collection) {
//        recommender.refresh(collection);
//    }
//}
