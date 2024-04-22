package Verifier;
import Graph.Node.TransactionLT;
import Graph.Operator.WriteLT;

public class Constants {
    public static String dataRoot = System.getProperty("user.dir")+"/data/";

    public final static TransactionLT initTxn = new TransactionLT(Graph.Txn.INIT_TXN,-1);
    public final static WriteLT initWrite = new WriteLT(Graph.Txn.INIT_OP,initTxn,-1);
}
