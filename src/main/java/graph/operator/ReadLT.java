package graph.operator;

import graph.vertex.TransactionLT;

import java.nio.ByteBuffer;

public class ReadLT extends OperatorLT {
    public long key;
    public long readFromTxn;
    public long readFromWop;

    public ReadLT(long opId, TransactionLT parent, long key, long readFromTxn, long readFromWop) {
        super(opId, parent);
        this.key = key;
        this.readFromTxn = readFromTxn;
        this.readFromWop = readFromWop;
    }

    @Override
    public byte[] toBytes(){
        int size = 1 + 4 * Long.BYTES;
        ByteBuffer entry;
        entry = ByteBuffer.allocate(size);
        entry.put((byte) 'R');
        entry.putLong(opId);
        entry.putLong(key);
        entry.putLong(readFromTxn);
        entry.putLong(readFromWop);
        return entry.array();
    }

}
