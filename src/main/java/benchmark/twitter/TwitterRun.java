package benchmark.twitter;

import benchmark.*;
import benchmark.tpcc.TPCCTData;
import database.KeyValueDB;
import database.PostgreSQL;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

public class TwitterRun extends RunBench<TwitterTData> {

    public Terminal<TwitterTData> terminals;

    public static void run(Properties ini){
        new TwitterRun(ini);
    }

    public TwitterRun(Properties ini) {
        super(ini);
        /*
         * Load the JDBC driver and prepare the db and dbProps.
         */
        try {
            Class.forName(getProp(ini,"driver"));
        } catch (Exception e) {
            log.error("ERROR: cannot load JDBC driver - {}", e.getMessage());
            System.exit(1);
        }
        boolean reGenerate = Boolean.parseBoolean(getProp(ini,"regenerate"));//重新运行benchmark
        boolean reBuild = Boolean.parseBoolean(getProp(ini,"rebuild"));//重新生成数据库
        switch (dbType){
            case "postgres":
                String url = getProp(ini,"psqlUrlTwitter");
                String user= getProp(ini,"psqlUser");
                String password = getProp(ini,"psqlPassword");
                db = new PostgreSQL(url,user,password);
                break;
            case "rocksdb":
            default:
                break;
        }
        //不需要重新生成且加载成功历史
        if (!reGenerate){
            if(loadHistory()){
                return;
            }
        }
        if(reBuild){
            System.out.println("rebuild");
            rebuildDatabase();
        }

        now = System.currentTimeMillis();
        System.out.println("start benchmark");
        doBenchmark();
    }

    @Override
    public void doBenchmark() {
        super.log = LogManager.getLogger(this.getClass());
        /* Create the scheduler. */
        scheduler = new Scheduler<TwitterTData>(this);
        scheduler_thread = new Thread(this.scheduler);
        scheduler_thread.start();
//        /*
//         * Create the SUT and schedule the launch of the SUT threads.
//         */
        systemUnderTest = new TwitterSUT(this);
//        /*
//         * Launch the threads that generate the terminal input data.
//         */
        terminals = new TwitterTerminal(this);

        for (int m = 0; m < numMonkeys; m++) {
            try {
                terminals.monkeyThreads[m].join();
            } catch (InterruptedException ignored) {
            }
        }
        for (int m = 0; m < numSUTThreads; m++) {
            try {
                systemUnderTest.sutThreads[m].join();
            } catch (InterruptedException ignored) {
            }
        }
        try {
            scheduler_thread.join();
            log.info("scheduler returned");
        } catch (InterruptedException e) {
            log.error("InterruptedException: {}", e.getMessage());
        }


    }

    @Override
    public boolean loadHistory() {
        super.log = LogManager.getLogger(this.getClass());
        return false;
    }

    @Override
    public void rebuildDatabase() {
        super.log = LogManager.getLogger(this.getClass());
        TwitterLoad.run(db,numWorkers);
    }


    @Override
    public void terminateAll() {
        terminals.terminate();
        scheduler.at(now, Scheduler.SCHED_DONE, getTData());
        systemUnderTest.terminate();
    }

    @Override
    public TwitterTData getTData() {
        return new TwitterTData();
    }

    @Override
    public DataList<TwitterTData> getDataList() {
        return new TwitterDataList();
    }
}
