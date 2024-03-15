package benchmark.twitter;

import javax.swing.plaf.PanelUI;

public class TwitterConfig {

    public static int UserNum = 1000;
    public static int tweetsPerUser = 10;

    public static double followWeight=40;
    public static double newTweetWeight = 20;
    public static double showFollowWeight=10;
    public static double showTweetsWeight=20;
    public static double timelineWeight=10;

    public static String outputPath = System.getProperty("user.dir")+"/output/ctwitter/";


    public static class TUser{
        public static String table = "users";
        public static String name = "name";
        public static String info = "info";
    }
    public static class TTweet{
        public static String table = "tweet";
        public static String author = "author";
        public static String data = "data";
    }
    public static class TLastTweet{
        public static String table = "last_tweet";
        public static String lastTweetId = "last_tweet_id";

    }
    public static class TFollowList{
        public static String table = "follow_list";
        public static String data = "data";

    }
    public static class TFollowers{
        public static String table ="followers";
        public static String time = "time";
        public static String follow = "follow";
    }
    public static class TFollowing{
        public static String table ="following";
        public static String time = "time";
        public static String follow = "follow";
    }

}
