package benchmark.tpcc;

import benchmark.DataList;
import benchmark.RunBench;
import benchmark.Scheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.util.Properties;

public class TPCCRun extends RunBench<TPCCTData> {
    public static int numWarehouses;
    public static int maxDeliveryBGThreads;
    public static int maxDeliveryBGPerWH;

    public static double newOrderWeight;
    public static double paymentWeight;
    public static double orderStatusWeight;
    public static double deliveryWeight;
    public static double stockLevelWeight;
    public static double rollbackPercent;

    public TPCCRandom rnd;
    public String applicationName;
    public String iDBType;
    public String iConn;
    public String iUser;
    public String iPassword;

    public static int loadWarehouses;
    public static int loadNuRandCLast;
    public static int loadNuRandCC_ID;
    public static int loadNuRandCI_ID;

    public String user;
    public String password;
    public String conn;

    public static int dbType;

    public TPCCRun(Properties ini){
        super(ini);
        super.log = LogManager.getLogger(this.getClass());
    }

    @Override
    public void doBenchmark() {

        /* Create the scheduler. */
//        scheduler = new Scheduler<TPCCTData>(this);
//        scheduler_thread = new Thread(this.scheduler);
//        scheduler_thread.start();
//
//        /*
//         * Create the SUT and schedule the launch of the SUT threads.
//         */
//        systemUnderTest = new TPCCSUT(this);
//
//        /*
//         * Launch the threads that generate the terminal input data.
//         */
//        terminals = new TPCCTerminal(this);
    }

    @Override
    public boolean loadHistory() {
        return false;
    }
    @Override
    public void rebuildDatabase() {
        TPCCLoad.load(ini);
    }
    @Override
    public void terminateAll() {
//        terminals.terminate();
//        scheduler.at(now, Scheduler.SCHED_DONE, getTData());
//        try {
//            scheduler_thread.join();
//            log.info("scheduler returned");
//        } catch (InterruptedException e) {
//            log.error("InterruptedException: {}", e.getMessage());
//        }
//        systemUnderTest.terminate();
    }
    @Override
    public TPCCTData getTData() {
        return new TPCCTData();
    }
    @Override
    public DataList<TPCCTData> getDataList() {
        return new TPCCDataList();
    }
}



