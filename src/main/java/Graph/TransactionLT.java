package Graph;

import Verifier.Graph;

import java.util.ArrayList;

public class TransactionLT {
    public final static long INIT_TXN = -2;
    public long clientId;
    public long txnId;

    public long start;
    public long end;
    public ArrayList<OperatorLT> Ops;

    public TransactionLT(long txnId, long clientId) {
        Ops = new ArrayList<>();
        this.txnId = txnId;
        this.clientId = clientId;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void appendOp(OperatorLT op) {
        Ops.add(op);
    }
}
