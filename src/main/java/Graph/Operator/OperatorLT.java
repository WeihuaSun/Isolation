package Graph.Operator;

import Graph.Node.TransactionLT;

public abstract class OperatorLT {
    public long opId;
    public TransactionLT parent;

    public OperatorLT(long opId,TransactionLT parent){
        this.opId = opId;
        this.parent = parent;
    }
    public abstract byte[] toBytes();

}
