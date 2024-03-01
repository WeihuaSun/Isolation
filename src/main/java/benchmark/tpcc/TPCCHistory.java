package benchmark.tpcc;

import benchmark.History;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TPCCHistory extends History {
    private static final Logger log = LogManager.getLogger(TPCCHistory.class);
    public String hDir;
    public String dbType;
    public String connection;
    public String user;
    public String password;

    public static int numWarehouses = 1;//fixed 1

    public static int numTerminals;
    public static int numSUTThreads;

    public static int numTransactions;
    public static int maxDeliveryBGThreads;
    public static int maxDeliveryBGPerWH;

    public static int randomSeed;

    public void genHistory(BufferedReader br){

    }

    public TPCCHistory(Properties ini){
        //Step1:检查历史文件是否存在

        //对于TPCC，我们固定使用一个Warehouses，正如大部分论文所使用的，
        dbType = getProp(ini,"db");
        user = getProp(ini,"user");
        password = getProp(ini,"password");
        connection = getProp(ini,"connection");

        numTerminals = Integer.parseInt(getProp(ini,"terminals"));//客户端数量
        numSUTThreads = Integer.parseInt(getProp(ini, "sutThreads"));//数据库连接数
        numTransactions = Integer.parseInt(getProp(ini, "transactions"));//事务数量，e.g.3k
        randomSeed = Integer.parseInt(getProp(ini,"seed"));

        boolean reGenerate = Boolean.parseBoolean(getProp(ini,"regenerate"));//重新运行benchmark
        boolean reBuild = Boolean.parseBoolean(getProp(ini,"rebuild"));//重新生成数据库

        if(reGenerate){
            Connection dbConn;
            try{
                Properties dbProps;
                dbProps = new Properties();
                dbProps.setProperty("user",user);
                dbProps.setProperty("password", password);
                dbConn = DriverManager.getConnection(connection, dbProps);
                if(reBuild){
                    TPCCLoad.load(ini);
                }
                //TPCCRun.run();

            }catch (SQLException se){
                log.error("can not connect to database");
            }

        }

        //历史文件命名规则:tpcc_dbType_numConn_numTerm_numTransactions,e.g.tpcc_postgres_8_16_3000_6666
        String hName = String.format("%s_%d_%d_%d_%d",dbType,numSUTThreads,numTerminals,numTransactions,randomSeed);

        InputStream  is = this.getClass().getResourceAsStream("history/tpcc/"+hName);
        if (is == null){//文件不存在，重新运行benchmark创建历史
            //todo:按照配置文件运行benchmark

        }else{//文件存在，则将其加载为历史
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            genHistory(br);
        }

    }
    private String getProp(Properties p, String pName) {
        String prop = p.getProperty(pName);
        log.info("main, {}={}", pName, prop);
        return (prop);
    }

}
