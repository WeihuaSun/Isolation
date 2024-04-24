package benchmark;

import Verifier.checker.Graph;

public class TData<T> {
    public final static int TT_NONE = 6, TT_DONE = 7;
    public int globalTxnId = 0;
    public T term_left;
    public T term_right;
    public long trans_due;
    public int trans_type;
    public int tree_height;
    public long trans_start;
    public long trans_end;
    public boolean trans_error;
    public int sched_code;
    public long sched_fuzz;
    public int terminalId;

    public Graph.Txn rs;

    //public abstract String dumpHdr();

}
