package benchmark.twitter;

import benchmark.RunBench;
import benchmark.Terminal;
import benchmark.tpcc.TPCCTerminal;
import org.apache.logging.log4j.LogManager;

import java.util.Random;

public class TwitterTerminal extends Terminal<TwitterTData> {
    public TwitterTData[] terminal_data;
    private final Random random = new Random(System.currentTimeMillis());
    public TwitterTerminal(RunBench<TwitterTData> rdata) {
        super(rdata);
        log = LogManager.getLogger(TPCCTerminal.class);
        terminal_data = new TwitterTData[rdata.numTerminals];
        for (int t = 0; t < rdata.numTerminals; t++) {
            terminal_data[t] = new TwitterTData();
            terminal_data[t].terminalId = t;
            terminal_data[t].trans_type = TwitterTData.TT_NONE;
            terminal_data[t].trans_due = rdata.now;
            terminal_data[t].trans_start = terminal_data[t].trans_due;
            terminal_data[t].trans_end = terminal_data[t].trans_due;
            terminal_data[t].trans_error = false;
            queueAppend(terminal_data[t]);
        }
    }

    @Override
    public double processResult(TwitterTData tdata) {
        if (tdata.trans_type == TwitterTData.TT_NONE)
            return 0.0;
        else
            return 2;//time
    }
    private double randomDouble() {
        return this.random.nextDouble();
    }
    private int nextTransactionType() {
        double chance = randomDouble() * 100.0;

        if (chance <= TwitterConfig.followWeight)
            return TwitterTData.TT_FOLLOW;
        chance -= TwitterConfig.followWeight;

        if (chance <= TwitterConfig.newTweetWeight)
            return TwitterTData.TT_NEW_TWEET;
        chance -= TwitterConfig.newTweetWeight;

        if (chance <= TwitterConfig.showFollowWeight)
            return TwitterTData.TT_SHOW_FOLLOW;
        chance -= TwitterConfig.showFollowWeight;

        if (chance <= TwitterConfig.showTweetsWeight)
            return TwitterTData.TT_SHOW_TWEETS;

        return TwitterTData.TT_TIMELINE;

    }

    @Override
    public double generateNew(TwitterTData tdata) {
        /*
         * Select the next transaction type.
         */
        tdata.trans_type = nextTransactionType();
        switch (tdata.trans_type){
            case TwitterTData.TT_FOLLOW:
                generateFollow(tdata);
                break;
            case TwitterTData.TT_NEW_TWEET:
                generateNewTweet(tdata);
                break;
            case TwitterTData.TT_SHOW_FOLLOW:
                generateShowFollow(tdata);
                break;
            case TwitterTData.TT_SHOW_TWEETS:
                generateShowTweets(tdata);
                break;
            case TwitterTData.TT_TIMELINE:
                generateTimeline(tdata);
                break;
            default:
                break;
        }

        return 0;
    }
    private void generateFollow(TwitterTData tdata) {
        TwitterTData.FollowData screen = tdata.FollowData();
        screen.genInputData();
        tdata.follow = screen;
    }
    private void generateNewTweet(TwitterTData tdata) {
        TwitterTData.NewTweetData screen = tdata.NewTweetData();
        screen.genInputData(tdata.terminalId);
        tdata.new_tweet = screen;
    }
    private void generateShowFollow(TwitterTData tdata){
        TwitterTData.ShowFollowData screen = tdata.ShowFollowData();
        screen.genInputData();
        tdata.show_follow = screen;
    }
    private void generateShowTweets(TwitterTData tdata){
        TwitterTData.ShowTweetsData screen = tdata.ShowTweetsData();
        screen.genInputData();
        tdata.show_tweets = screen;
    }
    private void generateTimeline(TwitterTData tdata){
        TwitterTData.TimelineData screen = tdata.TimelineData();
        screen.genInputData();
        tdata.timeline = screen;
    }
}
