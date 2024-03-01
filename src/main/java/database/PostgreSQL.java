package database;

import Verifier.Graph;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import benchmark.Utils;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class PostgreSQL implements KeyValueDB {
    public static Logger log = LogManager.getLogger(PostgreSQL.class);
    public String url;
    public String user;
    public String password;
    public Properties dbProps;
    public Map<Integer, PostgreSQLConnection> tid2conn;
    public long numOps;

    public PostgreSQL(String url,String user,String password){
        this.url = url;
        this.password = password;
        this.user = user;
        this.numOps = 0;
        tid2conn = new ConcurrentHashMap<>();
    }
    public synchronized long increment(){
        numOps++;
        return numOps;
    }
    private Connection createConnection(){
        Connection dbConn;
        dbProps = new Properties();
        dbProps.setProperty("user", user);
        dbProps.setProperty("password", password);
        try {
            dbConn = DriverManager.getConnection(url, dbProps);
            dbConn.setAutoCommit(false);
            return dbConn;
        }catch (SQLException se){
            log.error(se.getMessage());
            log.error("Can not connect to database {}",se.getMessage());
        }
        return null;
    }
    public PostgreSQLConnection getConnection(int id){
        if (!tid2conn.containsKey(id))
            return newConnection(id);
        return tid2conn.getOrDefault(id, null);
    }
    public PostgreSQLConnection newConnection(int id){
        PostgreSQLConnection pConn = new PostgreSQLConnection(this,id);
        tid2conn.put(id,pConn);
        return pConn;
    }
    @Override
    public boolean connect(int num){
        for(int i=0;i<num;i++){
            newConnection(i);
        }
        return true;
    }
    @Override
    public Graph.Txn begin(int id,long txnId) {
        return getConnection(id).begin(txnId,id);
    }

    @Override
    public boolean commit(int id) {
        return getConnection(id).commit();
    }

    @Override
    public boolean abort(int id) {
        return getConnection(id).abort();
    }

    @Override
    public boolean rollback(int id) {
        return true;
    }

    @Override
    public boolean insert(int id, String key, String value,boolean pack) {
        return getConnection(id).insert(key,value,pack);
    }

    @Override
    public boolean delete(int id, String key) {
        return getConnection(id).delete(key);
    }

    @Override
    public String get(int id, String key) {

        return getConnection(id).get(key);
    }

    @Override
    public boolean set(int id, String key, String value) {

        return getConnection(id).set(key,value);
    }

    public static class PostgreSQLConnection{
        private final Connection dbConn;
        private final PostgreSQL parent;
        private Statement writeBuffer;
        private final int id;
        private Graph.Txn curTxn;
        private long opStart;
        private long opEnd;


        public PostgreSQLConnection(PostgreSQL parent,int id){
            this.parent = parent;
            this.id = id;
            dbConn = this.parent.createConnection();
        }
        private long now(){return System.currentTimeMillis();}
        public Graph.Txn begin(long txnId,int id){
            parent.increment();
            this.curTxn = new Graph.Txn(txnId,id);
            this.curTxn.setStart(now());
            return this.curTxn;
        }
        public boolean commit(){
            try {
                opStart = now();
                dbConn.commit();
                opEnd = now();
                Graph.Commit op = new Graph.Commit(parent.increment(),curTxn.txnId,opStart,opEnd);
                curTxn.appendOp(op);
                curTxn.setCommitted();
            }catch (SQLException se){
                log.error("commit: " + se.getMessage());
                this.abort();
                return false;
            }
            return true;
        }
        public boolean abort(){
            try {
                opStart = now();
                dbConn.rollback();
                opEnd = now();
                Graph.Abort op = new Graph.Abort( parent.increment(),curTxn.txnId,opStart,opEnd);
                curTxn.appendOp(op);
                curTxn.setAborted();
            }catch (SQLException se){
                log.error("abort: "+se.getMessage());
                return false;
            }
            return true;
        }
        public boolean insert(String pKey,String value,boolean pack){
            String[] table_key = Utils.decodeKey(pKey);
            String table = table_key[0];
            String key = table_key[1];
            long opId = parent.increment();
            if (pack){
                value = Utils.packVal(value,curTxn.txnId,opId);
            }
            try {
                opStart = now();
                Statement stmt = dbConn.createStatement();
                String sql = String.format("INSERT INTO " + table + " (key, value) VALUES ('%s', '%s')", key, value);
               //System.out.println(sql);
                int insertRows = stmt.executeUpdate(sql);
                opEnd = now();
                if (insertRows == 0)
                    return false;
                Graph.Write op = new Graph.Write(opId,curTxn.txnId,opStart,opEnd,pKey,value);
                curTxn.appendOp(op);
            }catch (SQLException se){
                log.error("insert: "+se.getMessage());
                abort();
                return false;
            }
            return true;
        }
        /*
        * batch process
        * */
        public boolean insert(String table,String[] keys,String[] values){
            return true;
        }

        public boolean delete(String pKey){
            String[] table_key = Utils.decodeKey(pKey);
            String table = table_key[0];
            String key = table_key[1];
            try {
                opStart = now();
                Statement stmt = dbConn.createStatement();
                String sql = String.format("DELETE FROM " + table + "WHERE key = '%s' ", key);
               //System.out.println(sql);
                int deleteRows = stmt.executeUpdate(sql);
                opEnd = now();
                if (deleteRows == 0)
                    return  false;
                Graph.Write op = new Graph.Write(Graph.Txn.DELETE_OP,curTxn.txnId,opStart,opEnd,pKey,null);
                curTxn.appendOp(op);
            }catch (SQLException se){
                log.error("insert: "+se.getMessage());
                abort();
                return false;
            }
            return true;
        }

        public String get(String pKey) {
            String[] table_key = Utils.decodeKey(pKey);
            String table = table_key[0];
            String key = table_key[1];
            String value = null;
            try {
                opStart = now();
                Statement stmt = dbConn.createStatement();
                String sql = String.format("SELECT value FROM " + table + " WHERE key = '%s' ", key);
               //System.out.println(sql);
                ResultSet rs = stmt.executeQuery(sql);
                opEnd = now();
                while (rs.next()) {
                    value = rs.getString("value");//may be packed
                    Graph.Read op = new Graph.Read(parent.increment(),curTxn.txnId,opStart,opEnd,pKey,value);
                    curTxn.appendOp(op);
                    value = op.realVal;
                }
            } catch (SQLException se) {
                abort();
                log.error("query: " + se.getMessage());
            }
            return value;
        }
        public boolean set(String pKey,String value){
            long opId = parent.increment();
            String[] table_key = Utils.decodeKey(pKey);
            String table = table_key[0];
            String key = table_key[1];
            value = Utils.packVal(value,curTxn.txnId,opId);
            try {
                opStart = now();
                Statement stmt = dbConn.createStatement();
                String sql = String.format("INSERT INTO " + table + " VALUES ('%s', '%s') ON CONFLICT (key) DO UPDATE SET value = '%s'", key, value,value );
               //System.out.println(sql);
                stmt.execute(sql);
                opEnd = now();
                Graph.Write op = new Graph.Write(opId,curTxn.txnId,opStart,opEnd,pKey,value);
                curTxn.appendOp(op);
            } catch (SQLException se) {
                log.error("upsert: "+se.getMessage());
                abort();
                return false;
            }
            return true;
        }

    }
}
