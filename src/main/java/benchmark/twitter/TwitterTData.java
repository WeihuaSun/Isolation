package benchmark.twitter;

import benchmark.RandomUtils;
import benchmark.TData;
import benchmark.Utils;
import benchmark.benchUtils.ZipfianGenerator;

import java.util.ArrayList;

public class TwitterTData extends TData<TwitterTData> {

    //public int lastTweetId = 10000000;
    public final static int TT_FOLLOW = 0,TT_NEW_TWEET=1,TT_SHOW_FOLLOW =2,TT_SHOW_TWEETS=3,TT_TIMELINE = 4;

    public int lastTweetId = TwitterConfig.UserNum*TwitterConfig.tweetsPerUser;
    private final ZipfianGenerator zipf = new ZipfianGenerator(TwitterConfig.UserNum);
    private final ZipfianGenerator activateUserZipf= new ZipfianGenerator(TwitterConfig.UserNum,0.5);

    private final RandomUtils rand = new RandomUtils();
    public FollowData follow = null;
    public NewTweetData new_tweet = null;
    public ShowTweetsData show_tweets = null;
    public ShowFollowData show_follow = null;
    public TimelineData timeline = null;

    public FollowData FollowData(){return new FollowData();}
    public class FollowData{
        //input data
        public int src;
        public int dst;
        public boolean follow;
        public String time;
        //no output

        public void genInputData(){
            this.src = activateUserZipf.nextValue().intValue();
            this.dst = zipf.nextValue().intValue();
            this.follow = rand.nextBoolean();
            this.time = Utils.MakeTimeStamp();
        }
    }
    public NewTweetData NewTweetData(){
        return new NewTweetData();
    }
    public class NewTweetData{
        //input data
        public int userId;
        public int tweetId;
        public String data;
        //no output
        public void genInputData(int cid){
            this.userId = activateUserZipf.nextValue().intValue();
            lastTweetId++;
            this.tweetId = lastTweetId+cid*10000000;
            this.data = rand.getAString(120,120);
        }
    }
    public ShowFollowData ShowFollowData(){
        return new ShowFollowData();
    }
    public class ShowFollowData{
        //input data
        public int userId;
        //output
        public ArrayList<Integer> follows;
        public void genInputData(){
            this.userId = activateUserZipf.nextValue().intValue();
        }
    }

    public ShowTweetsData ShowTweetsData(){return  new ShowTweetsData();}
    public class ShowTweetsData{
        //input data
        public int userId;
        public ArrayList<Integer> ltids;

        //output data
        public int lastTweet;
        public void genInputData(){
            this.userId = activateUserZipf.nextValue().intValue();
        }

    }

    public TimelineData TimelineData(){return new TimelineData();}
    public class TimelineData{
        //input data
        public int userId;
        //output data
        public ArrayList<Integer> following;
        public ArrayList<Integer> ltids;
        public void genInputData(){
            this.userId = activateUserZipf.nextValue().intValue();
        }
    }
}
