package benchmark.twitter;


import Verifier.Graph;
import benchmark.SUT;
import benchmark.Utils;

import database.KeyValueDB;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

public class TwitterSUT extends SUT<TwitterTData> {

    private final KeyValueDB db;
    public TwitterRun rdata;

    public TwitterSUT(TwitterRun rdata) {
        super(rdata);
        this.rdata = rdata;
        log = LogManager.getLogger(TwitterSUT.class);
        this.db = rdata.db;
    }

    @Override
    public void processTransaction(TwitterTData tdata, int sut_id) {
        /* Process the requested transaction on the database. */
        Graph.Txn txn =  db.begin(sut_id,incrementTxn());
        String path;
        switch (tdata.trans_type){
            case TwitterTData.TT_FOLLOW:
                executeFollow(tdata.follow,sut_id);
                break;
            case TwitterTData.TT_NEW_TWEET:
                //tdata.rs = db.begin(sut_id,incrementTxn());
                executeNewTweet(tdata.new_tweet,sut_id);
                break;
            case TwitterTData.TT_SHOW_TWEETS:
                //tdata.rs = db.begin(sut_id,incrementTxn());
                executeShowTweets(tdata.show_tweets,sut_id);
                break;
            case TwitterTData.TT_SHOW_FOLLOW:
                //tdata.rs = db.begin(sut_id,incrementTxn());
                executeShowFollow(tdata.show_follow,sut_id);
                break;
            case TwitterTData.TT_TIMELINE:
                //tdata.rs = db.begin(sut_id,incrementTxn());
                executeTimeline(tdata.timeline,sut_id);
                break;
            default:
                log.error("sut unhandled Transaction type code {} in SUT", tdata.trans_type);
                break;
        }
        try {
            path = TwitterConfig.outputPath + sut_id + ".log";
            Utils.dumpLog(txn,path);
        }catch (IOException e){
            log.error("Dump log error!",e.getMessage());
        }
        //System.out.println(txn.txnId);
    }

    @Override
    public void afterTransaction(TwitterTData tdata) {
        rdata.terminals.queueAppend(tdata);
    }

    //Transaction:Follow
    private void executeFollow(TwitterTData.FollowData follow,int id){
        String key,value;
        //set followers
        int[] keys = {follow.dst, follow.src};
        key = Utils.encodeKey(TwitterConfig.TFollowers.table,keys);

        HashMap<String, String> tuple = new HashMap<String, String>();
        tuple.put(TwitterConfig.TFollowers.time, follow.time);
        tuple.put(TwitterConfig.TFollowers.follow, Boolean.toString(follow.follow));
        value = Utils.encodeTuple(tuple);
        db.set(id,key,value);

        //set following
        keys = new int[]{follow.src, follow.dst};
        key = Utils.encodeKey(TwitterConfig.TFollowing.table,keys);
        db.set(id,key,value);

        //update follow list
        keys = new int[]{follow.src};
        key = Utils.encodeKey(TwitterConfig.TFollowList.table,keys);
        value = db.get(id,key);
        if (value == null){
            db.commit(id);
            return;
        }
        tuple = Utils.decodeTuple(value);
        value = (String) tuple.get(TwitterConfig.TFollowList.data);

        byte[] bytes = Base64.getDecoder().decode(value);
        if (follow.follow){
            Utils.setBitMapAt(bytes,follow.dst);
        }else {
            Utils.clearBitMapAt(bytes, follow.dst);
        }
        String newVal = Base64.getEncoder().encodeToString(bytes);
        tuple.replace(key,newVal);
        newVal = Utils.encodeTuple(tuple);

        db.set(id,key,newVal);
        db.commit(id);
    }
    private void executeNewTweet(TwitterTData.NewTweetData newTweet,int id){

        String key;
        String value;
        //insert tweet
        int[] keys = {newTweet.tweetId};
        key = Utils.encodeKey(TwitterConfig.TTweet.table, keys);
        HashMap<String, String> tuple = new HashMap<>();
        tuple.put(TwitterConfig.TTweet.author,Integer.toString(newTweet.userId));
        tuple.put(TwitterConfig.TTweet.data,newTweet.data);
        value = Utils.encodeTuple(tuple);
        db.insert(id,key,value,true);
        //set last tweet
        keys = new int[]{newTweet.userId};
        key = Utils.encodeKey(TwitterConfig.TLastTweet.table,keys);
        tuple = new HashMap<>();
        tuple.put(TwitterConfig.TLastTweet.lastTweetId,Integer.toString(newTweet.tweetId));
        value = Utils.encodeTuple(tuple);
        db.set(id,key,value);
        db.commit(id);
    }
    private void executeShowFollow(TwitterTData.ShowFollowData showFollow ,int id){

        String key;
        String value;
        ArrayList<Integer> ret = new ArrayList<Integer>();
        int[] keys = { showFollow.userId };
        key = Utils.encodeKey(TwitterConfig.TFollowList.table,keys);
        value = db.get(id,key);
        if (value == null){
            db.commit(id);
            return;
        }
        HashMap<String, String> tuple = Utils.decodeTuple(value);
        value =  tuple.get(TwitterConfig.TFollowList.data);
        if (value!=null){
            byte[] bytes = Base64.getDecoder().decode(value);
            for (int i = 0; i < bytes.length * 8; i++) {
                if (Utils.getBitMapAt(bytes, i)) {
                    ret.add(i);
                    if (ret.size() >= 20) {
                        break; // only look for 20 records
                    }
                }
            }
        }
        showFollow.follows = ret;
        db.commit(id);
    }

    private void executeShowTweets(TwitterTData.ShowTweetsData showTweet,int id){

        String key;
        String value;
        //get last tweet Id
        int[] keys = {showTweet.userId};
        key = Utils.encodeKey(TwitterConfig.TLastTweet.table,keys);
        value = db.get(id,key);
        if (value == null){
            db.commit(id);
            return;
        }
        HashMap<String,String> tuple = Utils.decodeTuple(value);
        int tweetId = Integer.parseInt(tuple.get(TwitterConfig.TLastTweet.lastTweetId));
        // only show 10 tweets
        int i = Math.max(tweetId - 10, 0);
        while(i <= tweetId) {
            keys = new int[]{i};
            key = Utils.encodeKey(TwitterConfig.TTweet.table,keys);
            value = db.get(id,key);
            i++;
        }
        db.commit(id);
    }
    private void executeTimeline(TwitterTData.TimelineData timeline,int id){

        String key;
        String value;

        ArrayList<Integer> follows = new ArrayList<Integer>();
        int[] keys = { timeline.userId };
        key = Utils.encodeKey(TwitterConfig.TFollowList.table,keys);
        value = db.get(id,key);
        if (value == null){
            db.commit(id);
            return;
        }
        HashMap<String, String> tuple = Utils.decodeTuple(value);
        String ret = (String) tuple.get(TwitterConfig.TFollowList.data);
        if (ret!=null){
            byte[] bytes = Base64.getDecoder().decode(ret);
            for (int i = 0; i < bytes.length * 8; i++) {
                if (Utils.getBitMapAt(bytes, i)) {
                    follows.add(i);
                    if (follows.size() >= 20) {
                        break; // only look for 20 records
                    }
                }
            }
        }
        for (int dstId : follows) {
            //get last tweetId
            keys = new int[]{dstId};
            key = Utils.encodeKey(TwitterConfig.TLastTweet.table,keys);
            value = db.get(id,key);
            if (value == null){
                db.commit(id);
                return;
            }
            tuple = Utils.decodeTuple(value);
            int tweetId = Integer.parseInt(tuple.get(TwitterConfig.TLastTweet.lastTweetId));
            // get the last tweet
            keys = new int[]{tweetId};
            key = Utils.encodeKey(TwitterConfig.TTweet.table,keys);
            value = db.get(id,key);
        }
        db.commit(id);
    }

}
