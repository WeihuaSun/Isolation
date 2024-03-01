package benchmark.tpcc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Formatter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TPCCLoad {
    public static void load(Properties ini){
        LoadData.run(ini);
    }
}


/**
 * LoadData - Load Sample Data directly into database tables or into CSV files using multiple
 * parallel workers.
 * Copyright (C) 2016, Denis Lussier Copyright (C) 2016, Jan Wieck
 */

class LoadData {
    private static final Logger log = LogManager.getLogger(LoadData.class);
    private static final StringBuffer sb = new StringBuffer();
    private static final Formatter fmt = new Formatter(sb);

    private static Properties ini ;
    private static String db;
    private static Properties dbProps;
    private static TPCCRandom rnd;
    private static String fileLocation = null;
    private static String csvNullValue = null;

    private static int numWarehouses;
    private static int numWorkers;

    private static boolean loadingItemDone = false;
    private static boolean loadingWarehouseDone = false;
    private static int nextWIDX = 0;
    private static int nextDIDX = 0;
    private static int nextOIDX = 0;
    private static int[][][] nextCID;
    private static final Object nextJobLock = new Object();

    private static LoadDataWorker[] workers;
    private static Thread[] workerThreads;

    private static final String[] argv = {};

    private static boolean writeCSV = false;
    private static BufferedWriter configCSV = null;
    private static BufferedWriter itemCSV = null;
    private static BufferedWriter warehouseCSV = null;
    private static BufferedWriter districtCSV = null;
    private static BufferedWriter stockCSV = null;
    private static BufferedWriter customerCSV = null;
    private static BufferedWriter historyCSV = null;
    private static BufferedWriter orderCSV = null;
    private static BufferedWriter orderLineCSV = null;
    private static BufferedWriter newOrderCSV = null;

    public static void run(Properties iini) {
        ini = iini;
        int i;

        log.info("Starting BenchmarkSQL LoadData");
        log.info("");

        /*
         * Load the Benchmark properties file.
         */
//        try {
//            ini.load(new FileInputStream(System.getProperty("prop")));
//        } catch (IOException e) {
//            log.error("ERROR: {}", e.getMessage());
//            System.exit(1);
//        }
        //argv = args;

        /*
         * Initialize the global Random generator that picks the C values for the load.
         */
        rnd = new TPCCRandom();

        /*
         * Load the JDBC driver and prepare the db and dbProps.
         */
        try {
            Class.forName(iniGetString("driver"));
        } catch (Exception e) {
            log.error("ERROR: cannot load JDBC driver - {}", e.getMessage());
            System.exit(1);
        }
        db = iniGetString("conn");
        dbProps = new Properties();
        dbProps.setProperty("user", iniGetString("user"));
        dbProps.setProperty("password", iniGetString("password"));

        /*
         * Parse other vital information from the props file.
         */
        numWarehouses = iniGetInt("warehouses");
        numWorkers = iniGetInt("loadWorkers", 4);
        fileLocation = iniGetString("fileLocation");
        csvNullValue = iniGetString("csvNullValue", "NULL");

        /*
         * If CSV files are requested, open them all.
         */
        if (fileLocation != null) {
            writeCSV = true;

            try {
                configCSV = new BufferedWriter(new FileWriter(fileLocation + "config.csv"));
                itemCSV = new BufferedWriter(new FileWriter(fileLocation + "item.csv"));
                warehouseCSV = new BufferedWriter(new FileWriter(fileLocation + "warehouse.csv"));
                districtCSV = new BufferedWriter(new FileWriter(fileLocation + "district.csv"));
                stockCSV = new BufferedWriter(new FileWriter(fileLocation + "stock.csv"));
                customerCSV = new BufferedWriter(new FileWriter(fileLocation + "customer.csv"));
                historyCSV = new BufferedWriter(new FileWriter(fileLocation + "cust-hist.csv"));
                orderCSV = new BufferedWriter(new FileWriter(fileLocation + "order.csv"));
                orderLineCSV = new BufferedWriter(new FileWriter(fileLocation + "order-line.csv"));
                newOrderCSV = new BufferedWriter(new FileWriter(fileLocation + "new-order.csv"));
            } catch (IOException ie) {
                log.error(ie.getMessage());
                System.exit(3);
            }
        }

        log.info("");

        /*
         * Initialize the random nextCID arrays (one per District) used in getNextJob()
         *
         * For the ORDER rows the TPC-C specification demands that they are generated using a random
         * permutation of all 3,000 customers. To do that we set up an array per district with all
         * C_IDs and randomly shuffle each.
         */
        nextCID = new int[numWarehouses][10][3000];
        for (int w_idx = 0; w_idx < numWarehouses; w_idx++) {
            for (int d_idx = 0; d_idx < 10; d_idx++) {
                for (int c_idx = 0; c_idx < 3000; c_idx++) {
                    nextCID[w_idx][d_idx][c_idx] = c_idx + 1;
                }
                for (i = 0; i < 3000; i++) {
                    int x = rnd.nextInt(0, 2999);
                    int y = rnd.nextInt(0, 2999);
                    int tmp = nextCID[w_idx][d_idx][x];
                    nextCID[w_idx][d_idx][x] = nextCID[w_idx][d_idx][y];
                    nextCID[w_idx][d_idx][y] = tmp;
                }
            }
        }

        /*
         * Create the number of requested workers and start them.
         */
        workers = new LoadDataWorker[numWorkers];
        workerThreads = new Thread[numWorkers];
        for (i = 0; i < numWorkers; i++) {
            Connection dbConn;

            try {
                dbConn = DriverManager.getConnection(db, dbProps);
                dbConn.setAutoCommit(false);
                if (writeCSV)
                    workers[i] = new LoadDataWorker(i, csvNullValue, rnd.newRandom());
                else
                    workers[i] = new LoadDataWorker(i, dbConn, rnd.newRandom());
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

        /*
         * Close the CSV files if we are writing them.
         */
        if (writeCSV) {
            try {
                configCSV.close();
                itemCSV.close();
                warehouseCSV.close();
                districtCSV.close();
                stockCSV.close();
                customerCSV.close();
                historyCSV.close();
                orderCSV.close();
                orderLineCSV.close();
                newOrderCSV.close();
            } catch (IOException ie) {
                log.error(ie.getMessage());
                System.exit(3);
            }
        }
    } // End of main()

    public static void configAppend(StringBuffer buf) throws IOException {
        synchronized (configCSV) {
            configCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void itemAppend(StringBuffer buf) throws IOException {
        synchronized (itemCSV) {
            itemCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void warehouseAppend(StringBuffer buf) throws IOException {
        synchronized (warehouseCSV) {
            warehouseCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void districtAppend(StringBuffer buf) throws IOException {
        synchronized (districtCSV) {
            districtCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void stockAppend(StringBuffer buf) throws IOException {
        synchronized (stockCSV) {
            stockCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void customerAppend(StringBuffer buf) throws IOException {
        synchronized (customerCSV) {
            customerCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void historyAppend(StringBuffer buf) throws IOException {
        synchronized (historyCSV) {
            historyCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void orderAppend(StringBuffer buf) throws IOException {
        synchronized (orderCSV) {
            orderCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void orderLineAppend(StringBuffer buf) throws IOException {
        synchronized (orderLineCSV) {
            orderLineCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static void newOrderAppend(StringBuffer buf) throws IOException {
        synchronized (newOrderCSV) {
            newOrderCSV.write(buf.toString());
        }
        buf.setLength(0);
    }

    public static LoadJob getNextJob() {
        synchronized (nextJobLock) {
            /*
             * The first job we kick off is loading the ITEM table.
             */
            if (!loadingItemDone) {
                LoadJob job = new LoadJob();
                job.type = LoadJob.LOAD_ITEM;

                loadingItemDone = true;

                return job;
            }

            /*
             * Load everything per warehouse except for the OORDER, ORDER_LINE and NEW_ORDER tables.
             */
            if (!loadingWarehouseDone) {
                if (nextWIDX <= numWarehouses) {
                    LoadJob job = new LoadJob();
                    job.type = LoadJob.LOAD_WAREHOUSE;
                    job.w_id = nextWIDX + 1;
                    nextWIDX += 1;

                    if (nextWIDX >= numWarehouses) {
                        nextWIDX = 0;
                        loadingWarehouseDone = true;
                    }

                    return job;
                }
            }

            /*
             * Load the OORDER, ORDER_LINE and NEW_ORDER rows.
             *
             * This is a state machine that will return jobs for creating orders in advancing O_ID
             * numbers. Within them it will loop trough W_IDs and D_IDs. The C_IDs will be the ones
             * preloaded into the nextCID arrays when this loader was created.
             */
            if (nextOIDX < 3000) {
                LoadJob job = new LoadJob();

                if (nextOIDX % 100 == 0 && nextDIDX == 0 && nextWIDX == 0) {
                    fmt.format("Loading Orders with O_ID %4d and higher", nextOIDX + 1);
                    log.info(sb.toString());
                    sb.setLength(0);
                }

                job.w_id = nextWIDX + 1;
                job.d_id = nextDIDX + 1;
                job.c_id = nextCID[nextWIDX][nextDIDX][nextOIDX];
                job.o_id = nextOIDX + 1;

                if (++nextDIDX >= 10) {
                    nextDIDX = 0;
                    if (++nextWIDX >= numWarehouses) {
                        nextWIDX = 0;
                        ++nextOIDX;

                        if (nextOIDX >= 3000) {
                            log.info("Loading Orders done");
                        }
                    }
                }

                fmt.format("Order %d,%d,%d,%d", job.o_id, job.w_id, job.d_id, job.c_id);
                // log.info(sb.toString());
                sb.setLength(0);
                return job;
            }

            /*
             * Nothing more to be done. Returning null signals to the worker to exit.
             */
            return null;
        }
    }

    public static int getNumWarehouses() {
        return numWarehouses;
    }

    private static String iniGetString(String name) {
        String strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2) {
            if (name.equalsIgnoreCase(argv[i])) {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null)
            log.warn("{} (not defined)", name);
        else if (name.equals("password"))
            log.info("{}=***********", name);
        else
            log.info("{}={}", name, strVal);
        return strVal;
    }

    private static String iniGetString(String name, String defVal) {
        String strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2) {
            if (name.equalsIgnoreCase(argv[i])) {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null) {
            log.warn("{} (not defined - using default '{}')", name, defVal);
            return defVal;
        } else if (name.equals("password"))
            log.info("{}=***********", name);
        else
            log.info("{}={}", name, strVal);
        return strVal;
    }

    private static int iniGetInt(String name) {
        String strVal = iniGetString(name);

        if (strVal == null)
            return 0;
        return Integer.parseInt(strVal);
    }

    private static int iniGetInt(String name, int defVal) {
        String strVal = iniGetString(name);

        if (strVal == null)
            return defVal;
        return Integer.parseInt(strVal);
    }
}

/**
 * LoadDataWorker - Class to load one Warehouse (or in a special case the ITEM table).
 * Copyright (C) 2016, Denis Lussier Copyright (C) 2016, Jan Wieck
 */
class LoadDataWorker implements Runnable {
    private static final Logger log = LogManager.getLogger(LoadDataWorker.class);

    private final int worker;
    private Connection dbConn;
    private final TPCCRandom rnd;

    private final StringBuffer sb;
    private final Formatter fmt;

    private boolean writeCSV = false;
    private String csvNull = null;

    private PreparedStatement stmtConfig = null;
    private PreparedStatement stmtItem = null;
    private PreparedStatement stmtWarehouse = null;
    private PreparedStatement stmtDistrict = null;
    private PreparedStatement stmtStock = null;
    private PreparedStatement stmtCustomer = null;
    private PreparedStatement stmtHistory = null;
    private PreparedStatement stmtOrder = null;
    private PreparedStatement stmtOrderLine = null;
    private PreparedStatement stmtNewOrder = null;

    private StringBuffer sbConfig = null;
    private Formatter fmtConfig = null;
    private StringBuffer sbItem = null;
    private Formatter fmtItem = null;
    private StringBuffer sbWarehouse = null;
    private Formatter fmtWarehouse = null;
    private StringBuffer sbDistrict = null;
    private Formatter fmtDistrict = null;
    private StringBuffer sbStock = null;
    private Formatter fmtStock = null;
    private StringBuffer sbCustomer = null;
    private Formatter fmtCustomer = null;
    private StringBuffer sbHistory = null;
    private Formatter fmtHistory = null;
    private StringBuffer sbOrder = null;
    private Formatter fmtOrder = null;
    private StringBuffer sbOrderLine = null;
    private Formatter fmtOrderLine = null;
    private StringBuffer sbNewOrder = null;
    private Formatter fmtNewOrder = null;

    LoadDataWorker(int worker, String csvNull, TPCCRandom rnd) {
        this.worker = worker;
        this.csvNull = csvNull;
        this.rnd = rnd;

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);
        this.writeCSV = true;

        this.sbConfig = new StringBuffer();
        this.fmtConfig = new Formatter(sbConfig);
        this.sbItem = new StringBuffer();
        this.fmtItem = new Formatter(sbItem);
        this.sbWarehouse = new StringBuffer();
        this.fmtWarehouse = new Formatter(sbWarehouse);
        this.sbDistrict = new StringBuffer();
        this.fmtDistrict = new Formatter(sbDistrict);
        this.sbStock = new StringBuffer();
        this.fmtStock = new Formatter(sbStock);
        this.sbCustomer = new StringBuffer();
        this.fmtCustomer = new Formatter(sbCustomer);
        this.sbHistory = new StringBuffer();
        this.fmtHistory = new Formatter(sbHistory);
        this.sbOrder = new StringBuffer();
        this.fmtOrder = new Formatter(sbOrder);
        this.sbOrderLine = new StringBuffer();
        this.fmtOrderLine = new Formatter(sbOrderLine);
        this.sbNewOrder = new StringBuffer();
        this.fmtNewOrder = new Formatter(sbNewOrder);
    }

    LoadDataWorker(int worker, Connection dbConn, TPCCRandom rnd) throws SQLException {
        this.worker = worker;
        this.dbConn = dbConn;
        this.rnd = rnd;

        this.sb = new StringBuffer();
        this.fmt = new Formatter(sb);

        stmtConfig = dbConn.prepareStatement(
                "INSERT INTO bmsql_config (" + "  cfg_name, cfg_value) " + "VALUES (?, ?)");
        stmtItem = dbConn.prepareStatement("INSERT INTO bmsql_item ("
                + "  i_id, i_im_id, i_name, i_price, i_data) " + "VALUES (?, ?, ?, ?, ?)");
        stmtWarehouse = dbConn.prepareStatement(
                "INSERT INTO bmsql_warehouse (" + "  w_id, w_name, w_street_1, w_street_2, w_city, "
                        + "  w_state, w_zip, w_tax, w_ytd) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmtStock = dbConn.prepareStatement(
                "INSERT INTO bmsql_stock (" + "  s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, "
                        + "  s_dist_03, s_dist_04, s_dist_05, s_dist_06, "
                        + "  s_dist_07, s_dist_08, s_dist_09, s_dist_10, "
                        + "  s_ytd, s_order_cnt, s_remote_cnt, s_data) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmtDistrict = dbConn.prepareStatement(
                "INSERT INTO bmsql_district (" + "  d_id, d_w_id, d_name, d_street_1, d_street_2, "
                        + "  d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmtCustomer = dbConn.prepareStatement("INSERT INTO bmsql_customer ("
                + "  c_id, c_d_id, c_w_id, c_first, c_middle, c_last, "
                + "  c_street_1, c_street_2, c_city, c_state, c_zip, "
                + "  c_phone, c_since, c_credit, c_credit_lim, c_discount, "
                + "  c_balance, c_ytd_payment, c_payment_cnt, " + "  c_delivery_cnt, c_data) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + "        ?, ?, ?, ?, ?, ?)");
        stmtHistory = dbConn.prepareStatement(
                "INSERT INTO bmsql_history (" + "  h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, "
                        + "  h_date, h_amount, h_data) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        stmtOrder = dbConn.prepareStatement(
                "INSERT INTO bmsql_oorder (" + "  o_id, o_d_id, o_w_id, o_c_id, o_entry_d, "
                        + "  o_carrier_id, o_ol_cnt, o_all_local) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        stmtOrderLine = dbConn.prepareStatement(
                "INSERT INTO bmsql_order_line (" + "  ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, "
                        + "  ol_supply_w_id, ol_delivery_d, ol_quantity, " + "  ol_amount, ol_dist_info) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmtNewOrder = dbConn.prepareStatement(
                "INSERT INTO bmsql_new_order (" + "  no_o_id, no_d_id, no_w_id) " + "VALUES (?, ?, ?)");
    }

    /*
     * run()
     */
    public void run() {
        LoadJob job;

        try {
            while ((job = LoadData.getNextJob()) != null) {
                if (job.type == LoadJob.LOAD_ITEM) {
                    fmt.format("Worker %03d: Loading ITEM", worker);
                    log.info(sb.toString());
                    sb.setLength(0);

                    loadItem();

                    fmt.format("Worker %03d: Loading ITEM done", worker);
                    log.info(sb.toString());
                    sb.setLength(0);
                } else if (job.type == LoadJob.LOAD_WAREHOUSE) {
                    fmt.format("Worker %03d: Loading Warehouse %6d", worker, job.w_id);
                    log.info(sb.toString());
                    sb.setLength(0);

                    loadWarehouse(job);

                    fmt.format("Worker %03d: Loading Warehouse %6d done", worker, job.w_id);
                    log.info(sb.toString());
                    sb.setLength(0);
                } else {
                    loadOrder(job);
                }
            }

            /*
             * Close the DB connection if in direct DB mode.
             */
            if (!writeCSV)
                dbConn.close();
        } catch (SQLException se) {
            while (se != null) {
                fmt.format("Worker %03d: ERROR: %s", worker, se.getMessage());
                log.error(sb.toString());
                sb.setLength(0);
                se = se.getNextException();
            }
        } catch (Exception e) {
            fmt.format("Worker %03d: ERROR: %s", worker, e.getMessage());
            log.error(sb.toString());
            sb.setLength(0);
            log.info(e);
        }
    } // End run()

    /*
     * ---- loadItem()
     *
     * Load the content of the ITEM table. ----
     */
    private void loadItem() throws SQLException, IOException {
        int i_id;

        if (writeCSV) {
            /*
             * Saving CONFIG information in CSV mode.
             */
            fmtConfig.format("warehouses,%d\n", LoadData.getNumWarehouses());
            fmtConfig.format("nURandCLast,%d\n", rnd.getNURandCLast());
            fmtConfig.format("nURandCC_ID,%d\n", rnd.getNURandCC_ID());
            fmtConfig.format("nURandCI_ID,%d\n", rnd.getNURandCI_ID());

            LoadData.configAppend(sbConfig);
        } else {
            /*
             * Saving CONFIG information in DB mode.
             */
            stmtConfig.setString(1, "warehouses");
            stmtConfig.setString(2, "" + LoadData.getNumWarehouses());
            stmtConfig.execute();

            stmtConfig.setString(1, "nURandCLast");
            stmtConfig.setString(2, "" + rnd.getNURandCLast());
            stmtConfig.execute();

            stmtConfig.setString(1, "nURandCC_ID");
            stmtConfig.setString(2, "" + rnd.getNURandCC_ID());
            stmtConfig.execute();

            stmtConfig.setString(1, "nURandCI_ID");
            stmtConfig.setString(2, "" + rnd.getNURandCI_ID());
            stmtConfig.execute();
        }

        for (i_id = 1; i_id <= 100000; i_id++) {
            String iData;

            if (i_id != 1 && (i_id - 1) % 1000 == 0) {
                if (writeCSV) {
                    LoadData.itemAppend(sbItem);
                } else {
                    stmtItem.executeBatch();
                    stmtItem.clearBatch();
                }
            }

            // Clause 4.3.3.1 for ITEM
            if (rnd.nextInt(1, 100) <= 10) {
                int len = rnd.nextInt(26, 50);
                int off = rnd.nextInt(0, len - 8);

                iData =
                        rnd.getAString(off, off) + "ORIGINAL" + rnd.getAString(len - off - 8, len - off - 8);
            } else {
                iData = rnd.getAString_26_50();
            }

            if (writeCSV) {
                fmtItem.format("%d,%s,%.2f,%s,%d\n", i_id, rnd.getAString_14_24(),
                        ((double) rnd.nextLong(100, 10000)) / 100.0, iData, rnd.nextInt(1, 10000));

            } else {
                stmtItem.setInt(1, i_id);
                stmtItem.setInt(2, rnd.nextInt(1, 10000));
                stmtItem.setString(3, rnd.getAString_14_24());
                stmtItem.setDouble(4, ((double) rnd.nextLong(100, 10000)) / 100.0);
                stmtItem.setString(5, iData);

                stmtItem.addBatch();
            }
        }

        if (writeCSV) {
            LoadData.itemAppend(sbItem);
        } else {
            stmtItem.executeBatch();
            stmtItem.clearBatch();
            stmtItem.close();

            dbConn.commit();
        }

    } // End loadItem()

    /*
     * ---- loadWarehouse()
     *
     * Load the content of one warehouse. ----
     */
    private void loadWarehouse(LoadJob job) throws SQLException, IOException {
        int w_id = job.w_id;

        /*
         * Load the WAREHOUSE row.
         */
        if (writeCSV) {
            fmtWarehouse.format("%d,%.2f,%.4f,%s,%s,%s,%s,%s,%s\n", w_id, 300000.0,
                    ((double) rnd.nextLong(0, 2000)) / 10000.0, rnd.getAString_6_10(), rnd.getAString_10_20(),
                    rnd.getAString_10_20(), rnd.getAString_10_20(), rnd.getState(),
                    rnd.getNString(4, 4) + "11111");

            LoadData.warehouseAppend(sbWarehouse);
        } else {
            stmtWarehouse.setInt(1, w_id);
            stmtWarehouse.setString(2, rnd.getAString_6_10());
            stmtWarehouse.setString(3, rnd.getAString_10_20());
            stmtWarehouse.setString(4, rnd.getAString_10_20());
            stmtWarehouse.setString(5, rnd.getAString_10_20());
            stmtWarehouse.setString(6, rnd.getState());
            stmtWarehouse.setString(7, rnd.getNString(4, 4) + "11111");
            stmtWarehouse.setDouble(8, ((double) rnd.nextLong(0, 2000)) / 10000.0);
            stmtWarehouse.setDouble(9, 300000.0);

            stmtWarehouse.execute();
        }

        /*
         * For each WAREHOUSE there are 100,000 STOCK rows.
         */
        for (int s_i_id = 1; s_i_id <= 100000; s_i_id++) {
            String sData;
            /*
             * Load the data in batches of 10,000 rows.
             */
            if (s_i_id != 1 && (s_i_id - 1) % 10000 == 0) {
                if (writeCSV)
                    LoadData.warehouseAppend(sbWarehouse);
                else {
                    stmtStock.executeBatch();
                    stmtStock.clearBatch();
                }
            }

            // Clause 4.3.3.1 for STOCK
            if (rnd.nextInt(1, 100) <= 10) {
                int len = rnd.nextInt(26, 50);
                int off = rnd.nextInt(0, len - 8);

                sData =
                        rnd.getAString(off, off) + "ORIGINAL" + rnd.getAString(len - off - 8, len - off - 8);
            } else {
                sData = rnd.getAString_26_50();
            }

            if (writeCSV) {
                fmtStock.format("%d,%d,%d,%d,%d,%d,%s," + "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", s_i_id, w_id,
                        rnd.nextInt(10, 100), 0, 0, 0, sData, rnd.getAString_24(), rnd.getAString_24(),
                        rnd.getAString_24(), rnd.getAString_24(), rnd.getAString_24(), rnd.getAString_24(),
                        rnd.getAString_24(), rnd.getAString_24(), rnd.getAString_24(), rnd.getAString_24());
            } else {
                stmtStock.setInt(1, s_i_id);
                stmtStock.setInt(2, w_id);
                stmtStock.setInt(3, rnd.nextInt(10, 100));
                stmtStock.setString(4, rnd.getAString_24());
                stmtStock.setString(5, rnd.getAString_24());
                stmtStock.setString(6, rnd.getAString_24());
                stmtStock.setString(7, rnd.getAString_24());
                stmtStock.setString(8, rnd.getAString_24());
                stmtStock.setString(9, rnd.getAString_24());
                stmtStock.setString(10, rnd.getAString_24());
                stmtStock.setString(11, rnd.getAString_24());
                stmtStock.setString(12, rnd.getAString_24());
                stmtStock.setString(13, rnd.getAString_24());
                stmtStock.setInt(14, 0);
                stmtStock.setInt(15, 0);
                stmtStock.setInt(16, 0);
                stmtStock.setString(17, sData);

                stmtStock.addBatch();
            }

        }
        if (writeCSV) {
            LoadData.stockAppend(sbStock);
        } else {
            stmtStock.executeBatch();
            stmtStock.clearBatch();
        }

        /*
         * For each WAREHOUSE there are 10 DISTRICT rows.
         */
        for (int d_id = 1; d_id <= 10; d_id++) {
            if (writeCSV) {
                fmtDistrict.format("%d,%d,%.2f,%.4f,%d,%s,%s,%s,%s,%s,%s\n", d_id, w_id, 30000.0,
                        ((double) rnd.nextLong(0, 2000)) / 10000.0, 3001, rnd.getAString_6_10(),
                        rnd.getAString_10_20(), rnd.getAString_10_20(), rnd.getAString_10_20(), rnd.getState(),
                        rnd.getNString(4, 4) + "11111");

                LoadData.districtAppend(sbDistrict);
            } else {
                stmtDistrict.setInt(1, d_id);
                stmtDistrict.setInt(2, w_id);
                stmtDistrict.setString(3, rnd.getAString_6_10());
                stmtDistrict.setString(4, rnd.getAString_10_20());
                stmtDistrict.setString(5, rnd.getAString_10_20());
                stmtDistrict.setString(6, rnd.getAString_10_20());
                stmtDistrict.setString(7, rnd.getState());
                stmtDistrict.setString(8, rnd.getNString(4, 4) + "11111");
                stmtDistrict.setDouble(9, ((double) rnd.nextLong(0, 2000)) / 10000.0);
                stmtDistrict.setDouble(10, 30000.0);
                stmtDistrict.setInt(11, 3001);

                stmtDistrict.execute();
            }

            /*
             * Within each DISTRICT there are 3,000 CUSTOMERs.
             */
            for (int c_id = 1; c_id <= 3000; c_id++) {
                if (writeCSV) {
                    fmtCustomer.format(
                            "%d,%d,%d,%.4f,%s,%s,%s," + "%.2f,%.2f,%.2f,%d,%d," + "%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                            c_id, d_id, w_id, ((double) rnd.nextLong(0, 5000)) / 10000.0,
                            (rnd.nextInt(1, 100) <= 90) ? "GC" : "BC",
                            (c_id <= 1000) ? rnd.getCLast(c_id - 1) : rnd.getCLast(), rnd.getAString_8_16(),
                            50000.00, -10.00, 10.00, 1, 0, rnd.getAString_10_20(), rnd.getAString_10_20(),
                            rnd.getAString_10_20(), rnd.getState(), rnd.getNString(4, 4) + "11111",
                            rnd.getNString(16, 16), new java.sql.Timestamp(System.currentTimeMillis()),
                            "OE", rnd.getAString_300_500());
                } else {
                    stmtCustomer.setInt(1, c_id);
                    stmtCustomer.setInt(2, d_id);
                    stmtCustomer.setInt(3, w_id);
                    stmtCustomer.setString(4, rnd.getAString_8_16());
                    stmtCustomer.setString(5, "OE");
                    if (c_id <= 1000)
                        stmtCustomer.setString(6, rnd.getCLast(c_id - 1));
                    else
                        stmtCustomer.setString(6, rnd.getCLast());
                    stmtCustomer.setString(7, rnd.getAString_10_20());
                    stmtCustomer.setString(8, rnd.getAString_10_20());
                    stmtCustomer.setString(9, rnd.getAString_10_20());
                    stmtCustomer.setString(10, rnd.getState());
                    stmtCustomer.setString(11, rnd.getNString(4, 4) + "11111");
                    stmtCustomer.setString(12, rnd.getNString(16, 16));
                    stmtCustomer.setTimestamp(13, new java.sql.Timestamp(System.currentTimeMillis()));
                    if (rnd.nextInt(1, 100) <= 90)
                        stmtCustomer.setString(14, "GC");
                    else
                        stmtCustomer.setString(14, "BC");
                    stmtCustomer.setDouble(15, 50000.00);
                    stmtCustomer.setDouble(16, ((double) rnd.nextLong(0, 5000)) / 10000.0);
                    stmtCustomer.setDouble(17, -10.00);
                    stmtCustomer.setDouble(18, 10.00);
                    stmtCustomer.setInt(19, 1);
                    stmtCustomer.setInt(20, 1);
                    stmtCustomer.setString(21, rnd.getAString_300_500());

                    stmtCustomer.addBatch();
                }

                /*
                 * For each CUSTOMER there is one row in HISTORY.
                 */
                if (writeCSV) {
                    fmtHistory.format("%d,%d,%d,%d,%d,%d,%s,%.2f,%s\n",
                            (w_id - 1) * 30000 + (d_id - 1) * 3000 + c_id, c_id, d_id, w_id, d_id, w_id,
                            new java.sql.Timestamp(System.currentTimeMillis()), 10.00,
                            rnd.getAString_12_24());
                } else {
                    stmtHistory.setInt(1, c_id);
                    stmtHistory.setInt(2, d_id);
                    stmtHistory.setInt(3, w_id);
                    stmtHistory.setInt(4, d_id);
                    stmtHistory.setInt(5, w_id);
                    stmtHistory.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));
                    stmtHistory.setDouble(7, 10.00);
                    stmtHistory.setString(8, rnd.getAString_12_24());

                    stmtHistory.addBatch();
                }
            }

            if (writeCSV) {
                LoadData.customerAppend(sbCustomer);
                LoadData.historyAppend(sbHistory);
            } else {
                stmtCustomer.executeBatch();
                stmtCustomer.clearBatch();
                stmtHistory.executeBatch();
                stmtHistory.clearBatch();
            }
        }

        if (!writeCSV)
            dbConn.commit();
    } // End loadWarehouse()

    /*
     * ---- loadOrder()
     *
     * Load one order, including order lines and new order rows. ----
     */
    private void loadOrder(LoadJob job) throws SQLException, IOException {
        int w_id = job.w_id;
        int d_id = job.d_id;
        int c_id = job.c_id;
        int o_id = job.o_id;

        int o_ol_cnt = rnd.nextInt(5, 15);

        if (writeCSV) {
            fmtOrder.format("%d,%d,%d,%d,%s,%d,%d,%s\n", o_id, w_id, d_id, c_id,
                    (o_id < 2101) ? rnd.nextInt(1, 10) : csvNull, o_ol_cnt, 1,
                    new java.sql.Timestamp(System.currentTimeMillis()));
        } else {
            stmtOrder.setInt(1, o_id);
            stmtOrder.setInt(2, d_id);
            stmtOrder.setInt(3, w_id);
            stmtOrder.setInt(4, c_id);
            stmtOrder.setTimestamp(5, new java.sql.Timestamp(System.currentTimeMillis()));
            if (o_id < 2101)
                stmtOrder.setInt(6, rnd.nextInt(1, 10));
            else
                stmtOrder.setNull(6, java.sql.Types.INTEGER);
            stmtOrder.setInt(7, o_ol_cnt);
            stmtOrder.setInt(8, 1);

            stmtOrder.addBatch();
        }

        /*
         * Create the ORDER_LINE rows for this ORDER.
         */
        for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            long now = System.currentTimeMillis();

            if (writeCSV) {
                fmtOrderLine.format("%d,%d,%d,%d,%d,%s,%.2f,%d,%d,%s\n", w_id, d_id, o_id, ol_number,
                        rnd.nextInt(1, 100000),
                        (o_id < 2101) ? new java.sql.Timestamp(now).toString() : csvNull,
                        (o_id < 2101) ? 0.00 : ((double) rnd.nextLong(1, 999999)) / 100.0, w_id, 5,
                        rnd.getAString_24());
            } else {
                stmtOrderLine.setInt(1, o_id);
                stmtOrderLine.setInt(2, d_id);
                stmtOrderLine.setInt(3, w_id);
                stmtOrderLine.setInt(4, ol_number);
                stmtOrderLine.setInt(5, rnd.nextInt(1, 100000));
                stmtOrderLine.setInt(6, w_id);
                if (o_id < 2101)
                    stmtOrderLine.setTimestamp(7, new java.sql.Timestamp(now));
                else
                    stmtOrderLine.setNull(7, java.sql.Types.TIMESTAMP);
                stmtOrderLine.setInt(8, 5);
                if (o_id < 2101)
                    stmtOrderLine.setDouble(9, 0.00);
                else
                    stmtOrderLine.setDouble(9, ((double) rnd.nextLong(1, 999999)) / 100.0);
                stmtOrderLine.setString(10, rnd.getAString_24());

                stmtOrderLine.addBatch();
            }
        }

        /*
         * The last 900 ORDERs are not yet delieverd and have a row in NEW_ORDER.
         */
        if (o_id >= 2101) {
            if (writeCSV) {
                fmtNewOrder.format("%d,%d,%d\n", w_id, d_id, o_id);
            } else {
                stmtNewOrder.setInt(1, o_id);
                stmtNewOrder.setInt(2, d_id);
                stmtNewOrder.setInt(3, w_id);

                stmtNewOrder.addBatch();
            }
        }

        if (writeCSV) {
            LoadData.orderAppend(sbOrder);
            LoadData.orderLineAppend(sbOrderLine);
            LoadData.newOrderAppend(sbNewOrder);
        } else {
            stmtOrder.executeBatch();
            stmtOrder.clearBatch();
            stmtOrderLine.executeBatch();
            stmtOrderLine.clearBatch();
            stmtNewOrder.executeBatch();
            stmtNewOrder.clearBatch();
        }

        if (!writeCSV)
            dbConn.commit();
    } // End loadOrder()
}

class LoadJob {
    public static final int LOAD_ITEM = 1;
    public static final int LOAD_WAREHOUSE = 2;
    public static final int LOAD_ORDER = 3;

    public int type;

    public int w_id;
    public int d_id;
    public int c_id;
    public int o_id;
}


