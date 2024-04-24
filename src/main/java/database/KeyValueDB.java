package database;

import Verifier.checker.Graph;

public interface KeyValueDB {
    boolean connect(int num);
    Graph.Txn begin(int id,long txnId);
    boolean commit(int id);
    boolean abort(int id);
    boolean rollback(int id);
    boolean insert(int id,String key,String value,boolean pack);
    boolean delete(int id,String key);
    String get(int id,String key);
    boolean set(int id,String key,String value);


}
