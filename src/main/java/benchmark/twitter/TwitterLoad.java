package benchmark.twitter;

import benchmark.RandomUtils;
import benchmark.Utils;
import database.KeyValueDB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;

public class TwitterLoad {
    private static final Logger log = LogManager.getLogger(TwitterLoad.class);
    public KeyValueDB db;

    public int numWorkers;
    private static final Object nextJobLock = new Object();
    private static int nextUserId = 0;

    public TwitterLoad(KeyValueDB db,int numWorkers){
        this.numWorkers = numWorkers;
        this.db = db;
    }
    public static void run(KeyValueDB db,int numWorkers){
        TwitterLoad loader = new TwitterLoad(db,numWorkers);
        loader.loadData();
    }
    public void loadData(){
        int i;
        LoadDataWorker[] workers = new LoadDataWorker[numWorkers];
        Thread[] workerThreads = new Thread[numWorkers];
        for ( i=0;i<numWorkers;i++){
            try {
                workers[i] = new LoadDataWorker(new RandomUtils(),this,i);
                workerThreads[i] = new Thread(workers[i]);
                workerThreads[i].start();
            } catch (SQLException se) {
                log.error("ERROR: {}", se.getMessage());
                System.exit(3);
                return;
            }

        }
        for (i = 0; i < numWorkers; i++) {
            try {
                workerThreads[i].join();
            } catch (InterruptedException ie) {
                log.error("ERROR: worker {} - {}", i, ie.getMessage());
                System.exit(4);
            }
        }


    }
    public static int getNextUserId(){
        synchronized (nextJobLock){
            if(nextUserId<TwitterConfig.UserNum){
                nextUserId++;
                System.out.println(nextUserId);
                return nextUserId;
            }
            else {
                return -1;
            }
        }
    }
}
class LoadDataWorker implements Runnable{
    private final int id;
    private final RandomUtils rnd;
    private final TwitterLoad parent;
    public LoadDataWorker(RandomUtils rnd,TwitterLoad parent,int id) throws SQLException {
        this.id = id;
        //this.dbConn = dbConn;
        this.rnd = rnd;
        this.parent = parent;
    }


    private void loadFollowing(int srcId,int dstId){
        int[] keys = { dstId, srcId };
        String key = Utils.encodeKey(TwitterConfig.TFollowing.table, keys);
        HashMap<String, String> tuple = new HashMap<>();
        tuple.put(TwitterConfig.TFollowing.time,"notime");
        tuple.put(TwitterConfig.TFollowing.follow,Boolean.toString(true));
        String value = Utils.encodeTuple(tuple);
        parent.db.insert(id,key,value,false);
    }
    private void loadFollowers(int srcId,int dstId){
        int[] keys = { dstId, srcId };
        String key = Utils.encodeKey(TwitterConfig.TFollowers.table, keys);
        HashMap<String, String> tuple = new HashMap<>();
        tuple.put(TwitterConfig.TFollowers.time,"notime");
        tuple.put(TwitterConfig.TFollowers.follow,Boolean.toString(true));
        String value = Utils.encodeTuple(tuple);
        parent.db.insert(id,key,value,false);
    }

    private void loadFollowList(int userId){
        int[] keys = new int[]{userId};
        String key = Utils.encodeKey(TwitterConfig.TFollowList.table, keys);
        byte[] bytes = new byte[2000];
        Utils.setBitMapAt(bytes, userId);
        String data= Base64.getEncoder().encodeToString(bytes);
        HashMap<String, String> tuple = new HashMap<>();
        tuple.put(TwitterConfig.TFollowList.data,data);
        String value = Utils.encodeTuple(tuple);
        parent.db.insert(id,key,value,false);
        parent.db.commit(id);
        loadFollowing(userId,userId);
        parent.db.commit(id);
        loadFollowers(userId,userId);
        parent.db.commit(id);
    }
    private void loadLastTweet(int tweetId,int userId){
        int[] keys = {userId};
        String key = Utils.encodeKey(TwitterConfig.TLastTweet.table,keys);
        HashMap<String, String> tuple = new HashMap<>();
        tuple.put(TwitterConfig.TLastTweet.lastTweetId,Integer.toString(tweetId));
        String value = Utils.encodeTuple(tuple);
        parent.db.insert(id,key,value,false);
        parent.db.commit(id);
        loadFollowList(userId);
    }
    private void loadTweet(int userId){
        int tweetId = -1;
        for (int i = 0; i < TwitterConfig.tweetsPerUser; i++) {
            String data = rnd.getAString(100,100);
            tweetId= userId * TwitterConfig.tweetsPerUser + i;
            int[] keys = new int[]{tweetId};
            String key = Utils.encodeKey(TwitterConfig.TTweet.table, keys);
            HashMap<String, String> tuple = new HashMap<>();
            tuple.put(TwitterConfig.TTweet.author,Integer.toString(userId));
            tuple.put(TwitterConfig.TTweet.data,data);
            String value = Utils.encodeTuple(tuple);
            parent.db.insert(id,key,value,false);
        }
        parent.db.commit(id);
        loadLastTweet(tweetId,userId);
    }
    private void loadUser(int userId){
        parent.db.begin(id,-3);
        String name = rnd.getAString(10,10);
        String info = rnd.getAString(200,200);
        int[] keys = { userId };
        String key = Utils.encodeKey(TwitterConfig.TUser.table, keys);
        HashMap<String, String> tuple = new HashMap<>();
        tuple.put(TwitterConfig.TUser.name,name);
        tuple.put(TwitterConfig.TUser.info,info);
        String value =Utils.encodeTuple(tuple);
        parent.db.insert(id,key,value,false);
        parent.db.commit(id);
        loadTweet(userId);
    }

    @Override
    public void run() {
        int userId;
        while ((userId = TwitterLoad.getNextUserId())!=-1){
            loadUser(userId);
        }
    }
}
