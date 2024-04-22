package Graph.Operator;

import Graph.Node.TransactionLT;

import java.nio.ByteBuffer;

public class AbortLT extends OperatorLT {
    public AbortLT(long opId, TransactionLT parent) {
        super(opId, parent);
    }

    @Override
    public byte[] toBytes() {
        int size = 1 + Long.BYTES;
        ByteBuffer entry;
        entry = ByteBuffer.allocate(size);
        entry.put((byte) 'A');
        entry.putLong(opId);
        return entry.array();
    }
}
