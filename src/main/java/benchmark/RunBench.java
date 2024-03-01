package benchmark;

import benchmark.tpcc.TPCCLoad;

import benchmark.twitter.TwitterTData;
import database.KeyValueDB;
import database.PostgreSQL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.swing.plaf.PanelUI;
import java.util.Formatter;
import java.util.Properties;

public abstract class RunBench<T extends TData<T>> {

    public int numTerminals;//客户端数量
    public int numSUTThreads;//SUT线程数量
    public int numMonkeys;
    public int numTransactions;
    public int numWorkers;

    public boolean historyDone = false;
    public long now;
    public Logger log = LogManager.getLogger(RunBench.class);
    public Properties ini;

    public SUT<T> systemUnderTest;
    public Scheduler<T> scheduler;
    public Thread scheduler_thread;
    public String dbType;
    public KeyValueDB db;


    protected String getProp(Properties p, String pName) {
        String prop = p.getProperty(pName);
        //log.info("main, {}={}", pName, prop);
        return (prop);
    }

    public RunBench(Properties ini) {
        this.ini = ini;
        numTerminals = Integer.parseInt(getProp(ini,"terminals"));//客户端数量
        numSUTThreads = Integer.parseInt(getProp(ini, "sutThreads"));//数据库连接数
        numMonkeys =Integer.parseInt(getProp(ini, "monkeys"));
        numTransactions = Integer.parseInt(getProp(ini, "transactions"));//事务数量，e.g.3k
        numWorkers = Integer.parseInt(getProp(ini, "workers"));
        dbType = getProp(ini,"db");
    }

    public abstract void doBenchmark();
    public abstract boolean loadHistory();
    public abstract void rebuildDatabase();
    public abstract T getTData();

    public abstract void terminateAll();

    public abstract DataList<T> getDataList();

}
