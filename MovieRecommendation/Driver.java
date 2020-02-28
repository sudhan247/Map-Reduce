public class Driver {
    public static void main(String[] args) throws Exception {
        RatingDataReader ratingDataReader = new RatingDataReader();
        UserRatingAveraging userRatingAveraging = new UserRatingAveraging();
        String rawRatingDataInputPath = args[0];
        String aggregatedUserRatingPath = args[1];
        String userAverageRatingPath = args[2];
        

        // job1: RatingDataReader.java
        String[] job1Args = {rawRatingDataInputPath, aggregatedUserRatingPath};
        ratingDataReader.main(job1Args);

        //job4: UserRatingAveraging.java
        String[] job4Args = {rawRatingDataInputPath, userAverageRatingPath};
        userRatingAveraging.main(job4Args);

        // execution:
        // hadoop jar recommender.jar Driver /input /output/userRating /output/cooccurrence /output/normalization /output/averageRating /output/multiplication /output/sum
    }
}
