package Verifier;
import benchmark.Utils;
import org.jgrapht.graph.*;
import org.jgrapht.traverse.DepthFirstIterator;


import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Graph  {


    public DirectedMultigraph<Txn,DefaultEdge> under;
    public DirectedMultigraph<Txn,DefaultEdge> over;


    public Graph(){
        this.under = new DirectedMultigraph<>(DefaultEdge.class);
        this.over = new DirectedMultigraph<>(DefaultEdge.class);
    }

    public static class Edge extends DefaultEdge{
        public int type;
        public Edge(int type){
            this.type = type;
        }
    }
    /*
    * Transaction node.
    * */

    public static class Begin extends TxnOp{
        public Begin(long opId,long txnId,long start, long end) {
            super(opId,txnId,start, end);
        }
        public byte[] toByte(){
            int size = 1 + 4 * Long.BYTES;
            ByteBuffer entry;
            entry = ByteBuffer.allocate(size);
            entry.put((byte) 'B');
            entry.putLong(txnId);
            entry.putLong(opId);
            entry.putLong(start);
            entry.putLong(end);
            return entry.array();
        }
    }

    public static class Commit extends TxnOp{
        public Commit(long opId,long txnId,long start, long end) {
            super(opId,txnId,start, end);
        }
        public byte[] toByte(){
            int size = 1 + 4 * Long.BYTES;
            ByteBuffer entry;
            entry = ByteBuffer.allocate(size);
            entry.put((byte) 'C');
            entry.putLong(txnId);
            entry.putLong(opId);
            entry.putLong(start);
            entry.putLong(end);
            return entry.array();
        }
    }

    public static class Abort extends TxnOp{
        public Abort(long opId,long txnId,long start, long end) {
            super(opId,txnId,start, end);
        }
        public byte[] toByte(){
            int size = 1 + 4 * Long.BYTES;
            ByteBuffer entry;
            entry = ByteBuffer.allocate(size);
            entry.put((byte) 'A');
            entry.putLong(txnId);
            entry.putLong(opId);
            entry.putLong(start);
            entry.putLong(end);
            return entry.array();
        }
    }
    public static class Read extends TxnOp implements Comparable<Read>{
        public Txn parent;
        public long key;
        public long val;
        public long readFromTxn;
        public long readFromWop;
        public String realVal;


        public Read(long opId,long txnId,long start, long end,String key,String val) {
            super(opId,txnId,start, end);
            this.key = Utils.hashString(key);
            this.val = Utils.hashString(val);
            Utils.ValInfo valInfo = Utils.unpackVal(val);
            if (valInfo ==null){
                this.realVal = val;
                this.readFromTxn = Txn.INIT_TXN;
                this.readFromWop = Txn.INIT_OP;
            }
            else {
                this.realVal = valInfo.val;
                this.readFromWop = valInfo.wId;
                this.readFromTxn = valInfo.txnId;
            }
        }

        public Read(long opId,long txnId,long start, long end,long key,long val,long readFromTxn,long readFromWop){
            super(opId,txnId,start, end);
            this.key = key;
            this.val = val;
            this.readFromTxn = readFromTxn;
            this.readFromWop = readFromWop;
        }
        public byte[] toByte(){
            int size = 1 + 8 * Long.BYTES;
            ByteBuffer entry;
            entry = ByteBuffer.allocate(size);
            entry.put((byte) 'R');
            entry.putLong(txnId);
            entry.putLong(opId);
            entry.putLong(key);
            entry.putLong(val);
            entry.putLong(start);
            entry.putLong(end);
            entry.putLong(readFromTxn);
            entry.putLong(readFromWop);
            return entry.array();
        }

        @Override
        public int compareTo(Read o) {
            return Long.compare(this.parent.iEnd,o.parent.iEnd);
        }
    }
    public static class Write extends TxnOp{

        public long replaceTime = Long.MAX_VALUE;
        public long key;
        public Txn parent;
        public long val;
        public long sStart;
        public long sEnd;
        public Write(long opId,long txnId,long start, long end,String realKey,String realVal) {
            super(opId,txnId,start,end);
            this.key = Utils.hashString(realKey);
            this.val = Utils.hashString(realVal);
        }
        public Write(long opId,long txnId,long start, long end,long key,Long val,long sStart,long sEnd){
            super(opId,txnId,start,end);
            this.key = key;
            this.val = val;
            this.sStart =sStart;
            this.sEnd = sEnd;
        }
        public Write(long opId,long txnId){
            super(opId,txnId,0,0);
        }

        public byte[] toByte(){
            int size = 1 + 6 * Long.BYTES;
            ByteBuffer entry;
            entry = ByteBuffer.allocate(size);
            entry.put((byte) 'W');
            entry.putLong(txnId);
            entry.putLong(opId);
            entry.putLong(key);
            entry.putLong(val);
            entry.putLong(start);
            entry.putLong(end);
            return entry.array();
        }

    }
    public static class Txn{
        public final static int OnGoing = 1,Committed = 2,Aborted =3;
        public final static long INIT_OP=-1,INIT_TXN=-2,DELETE_OP = -3;
        public long clientId;
        public int txnType;
        public long txnId;
        public int state = OnGoing;
        public long sStart;
        public long start;
        public long sEnd;
        public long iStart;
        public long iEnd;

        public ArrayList<TxnOp> txnOps;

        public Txn(long txnId,long clientId){
            txnOps = new ArrayList<TxnOp>();
            this.txnId = txnId;
            this.clientId = clientId;
        }
        public void setStart(long sStart,long sEnd){
            this.sStart = sStart;
            this.sEnd = sEnd;
        }
        public void setEnd(long iStart,long iEnd){
            this.iStart = iStart;
            this.iEnd = iEnd;
        }


        public void appendOp(Graph.Write op){
            op.parent = this;
            txnOps.add(op);
        }
        public void appendOp(Graph.Read op){
            op.parent = this;
            txnOps.add(op);
        }
        public void appendOp(TxnOp op){
            txnOps.add(op);
        }
        public void setCommitted(){this.state = Committed;}
        public void setAborted(){this.state = Aborted;}

        @Override
        public String toString(){
            return "";
        }

    }
    public abstract static class TxnOp{
        public long opId;
        public long txnId;
        public long start;
        public long end;
        public TxnOp(long opId,long txnId,long start,long end){
            this.opId = opId;
            this.txnId = txnId;
            this.start = start;
            this.end = end;
        }
        public abstract byte[] toByte();
        public void setStart(long start){
            this.start = start;
        }
        public void setEnd(long end){
            this.end = end;
        }
    }

}



