package Graph.Operator;

import Graph.Node.TransactionLT;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Set;

public class WriteLT extends OperatorLT {
    public long key;
    public Set<TransactionLT> neighbors;
    public long replaceTime = Long.MAX_VALUE;
    public long wReplaceTime = Long.MAX_VALUE;

    public ArrayList<WritePair> wwPairs = new ArrayList<>();

    public WriteLT(long opId, TransactionLT parent, long key) {
        super(opId, parent);
        this.key = key;
    }

    public void setReplaceTime(long replaceTime){
        this.replaceTime = replaceTime;
    }
    public void setState(){
        wwPairs.removeIf(WritePair::checkState);
    }

    @Override
    public byte[] toBytes() {
        int size = 1 + 2 * Long.BYTES;
        ByteBuffer entry;
        entry = ByteBuffer.allocate(size);
        entry.put((byte) 'W');
        entry.putLong(opId);
        entry.putLong(key);
        return entry.array();
    }
}
