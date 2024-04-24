
package Verifier.checker;

import java.io.*;
import java.util.*;

import Verifier.Constants;
import Verifier.exception.ISException;
import Verifier.writePair.WriteLT;
import Verifier.writePair.WritePair;
import graph.*;

import graph.edge.DependencyEdge;
import graph.operator.OperatorLT;
import graph.operator.ReadLT;
import graph.vertex.TransactionLT;
import org.jgrapht.graph.*;


public class TxnLevelVerifier {
    public final int IS_S_SER = 6,IS_SSI = 5,IS_SER = 4,IS_SI = 3,IS_RR = 2,IS_RC = 1,IS_RU = 0;
    public int ISLevel;
    public ArrayList<ArrayList<TransactionLT>> history;

    public HashMap<Long,TreeSet<Long>> minReadTime;//对于每个Key而言，还未处理的最早的读取时间，该时间决定了写的replaceTime;如果minReadTime为空，则参考值为当前的checkStart

    public DirectedMultigraph<TransactionLT, DependencyEdge> over;//有向多重图
    public DependencyGraph<TransactionLT,DependencyEdge> under;//DAG不允许出现循环

    public HashMap<Long, LinkedList<WriteLT>> writeMap = new HashMap<>();//存储尚未过时的Write,即后续的

    //   public DirectedAcyclicGraph<TransactionLT,DefaultEdge> over;
    public HashMap<Long,HashMap<Long,ArrayList<WritePair>>> wwPairs;
    public ArrayList<WritePair> outdatedWritePairs;
    public ArrayList<WriteLT> savePoints;

    public TreeSet<WriteLT> toObsoleteWrite;//将要淘汰的

    public TreeSet<TransactionLT> aliveTxns;

    public String logDir;

    public long checkStart;

    public long checkEnd;

    public PriorityQueue<ReadLT> unKnownReads;

    public TxnLevelVerifier(String logDir,int ISLevel){
        this.ISLevel = ISLevel;
        this.logDir = logDir;
        unKnownReads = new PriorityQueue<>();
        over = new DirectedMultigraph<>(DependencyEdge.class);
        under = new DependencyGraph<TransactionLT, DependencyEdge>(DependencyEdge.class);
        loadHistory();
        mergeSort();
    }

    private void loadHistory(){
        history = new ArrayList<>();
        File logDir = new File(this.logDir);
        ArrayList<File> logFiles = new ArrayList<>();
        for (File f : Objects.requireNonNull(logDir.listFiles())) {
            if (f.isFile() && f.getName().endsWith("log")) {
                logFiles.add(f);
            }
        }
        try {
            int i = 0;
            for (File f : logFiles) {
                try (FileInputStream fi = new FileInputStream(f)) {
                    BufferedInputStream bf = new BufferedInputStream(fi, 1024 * 1024);
                    DataInputStream in = new DataInputStream(bf);
                    history.add(extractLog(in,i));
                }
                i++;
            }
        } catch (IOException ignored) {
        }
    }
    private ArrayList<TransactionLT> extractLog (DataInputStream in,int clientId)throws IOException{
        ArrayList<TransactionLT> txns = new ArrayList<>();
        char opType;
        long txnId,opId,start,end,key,value,readFromTxn,readFromOp;
        TransactionLT curTxn = new TransactionLT(TransactionLT.INIT_TXN,clientId);
        boolean newTxn = true;
        while (true){
            try {
                opType = (char) in.readByte();
            }catch (EOFException e){
                break;
            }
            switch(opType) {
                case 'C':
                    txnId = in.readLong();
                    opId = in.readLong();
                    start = in.readLong();
                    end = in.readLong();

                    curTxn.appendOp(new CommitLT(opId,curTxn));
                    curTxn.setEnd(end);
                    txns.add(curTxn);

                    newTxn = true;
                    if(curTxn.txnId!=txnId)
                        break;
                    break;
                case 'A':
                    System.out.println("ab");
                    txnId = in.readLong();
                    opId = in.readLong();
                    start = in.readLong();
                    end = in.readLong();
                    //op = new Graph.Abort(opId,txnId,start,end);
                    newTxn = true;
                    break;
                case 'W':
                    txnId = in.readLong();
                    opId = in.readLong();
                    key = in.readLong();
                    value = in.readLong();
                    start = in.readLong();
                    end = in.readLong();
                    if(newTxn){
                        curTxn = new TransactionLT(txnId,clientId);
                        curTxn.setStart(start);
                        newTxn = false;
                    }
                    if(curTxn.txnId!=txnId)
                        System.out.println("err");
                    curTxn.appendOp(new WriteLT(opId,curTxn,key));
                    break;
                case 'R':
                    txnId = in.readLong();
                    opId = in.readLong();
                    key = in.readLong();
                    value = in.readLong();
                    start = in.readLong();
                    end = in.readLong();
                    readFromTxn = in.readLong();
                    readFromOp = in.readLong();
                    if(newTxn){
                        curTxn = new TransactionLT(txnId,clientId);
                        curTxn.setStart(start);
                        newTxn = false;
                    }
                    if(curTxn.txnId!=txnId)
                        System.out.println("err");
                    curTxn.appendOp(new ReadLT(opId,curTxn,key,readFromTxn,readFromOp));
                    break;
                default:
                    break;
            }
        }
        return txns;
    }



    private void mergeSort(){
        System.out.println("merge");
        PriorityQueue<Element> minHeap = new PriorityQueue<>();
        for(int i=0;i<history.size();i++){
            ArrayList<TransactionLT> session = history.get(i);
            if(!session.isEmpty())
                minHeap.add(new Element(session.removeFirst(),i));
        }
        int i=0;
        long now = System.currentTimeMillis();
        while (!minHeap.isEmpty()){
            Element element = minHeap.poll();
            if(i%1000 == 0)
                System.out.println(System.currentTimeMillis()-now);
            //System.out.println(i);
            i++;
            try {
                feedTxn(element.value);
            } catch (ISException.InternalRead | ISException.ReadFromUnknown |
                     ISException.CycleException e) {
                //System.out.println(e.getMessage());
            }
            if(!history.get(element.rowIndex).isEmpty()){
                minHeap.add(new Element(history.get(element.rowIndex).removeFirst(),element.rowIndex));
            }
        }
        terminate();
    }

    /**
     *将合适的TO类型的边添加到DG中
     * @param t:当前处理的事务
     */
    public void timeOrder(TransactionLT t){
        //aliveTxns按照结束时间排序事务，结束时间最早的排在最前面
        TransactionLT prev = aliveTxns.first();
        while (!aliveTxns.isEmpty()&&prev.end<=checkStart){
            aliveTxns.removeFirst();
            addEdge(under,prev,t,"TO");
            addEdge(over,prev,t,"TO");
        }
    }

    public static class Element implements Comparable<OpLevelVerifier.Element> {
        TransactionLT value;
        int rowIndex;
        public Element(TransactionLT value, int rowIndex) {
            this.value = value;
            this.rowIndex = rowIndex;
        }
        @Override
        public int compareTo(OpLevelVerifier.Element other) {
            return Long.compare(this.value.start, other.value.start);
        }
    }
    private void feedTxn(TransactionLT curTxn) throws ISException.InternalRead, ISException.ReadFromUnknown, ISException.CycleException {
        under.addVertex(curTxn);
        over.addVertex(curTxn);
        checkStart = curTxn.start;
        checkEnd = curTxn.end;
        HashMap<Long, WriteLT> txnWrites = analyseTransaction(curTxn);//done
        //处理写
        sortWrite(txnWrites);
        //处理读
        findReadFrom();
        //处理过时
        removeObsoleteWrites();


    }

    private HashMap<Long,WriteLT> analyseTransaction(TransactionLT txn) throws ISException.InternalRead {
        HashMap<Long, WriteLT> localWrites = new HashMap<>();//key->WriteOp
        for(OperatorLT op:txn.Ops){
            if(op instanceof ReadLT read){
                WriteLT mayWrite = localWrites.get(read.key);
                if(mayWrite !=null){
                    if (mayWrite.opId != read.readFromWop)
                        throw new ISException.InternalRead();
                }
                else{
                    unKnownReads.add(read);
                    minReadTime.get(read.key).add(read.parent.start);
                }
            }else if(op instanceof WriteLT write){
                localWrites.put(write.key,write);
            }
        }
        return localWrites;
    }


    private void addEdge(DependencyGraph<TransactionLT, DependencyEdge> g, TransactionLT s, TransactionLT t, String type){
        try {
            g.addEdge(s,t,new DependencyEdge(type));
        }catch (GraphCycleProhibitedException e){

        }
    }



    private void findReadFrom() throws ISException.ReadFromUnknown, ISException.CycleException {
        //unKnownReads按照结束时间安排队列，结束时间最早的在队列前，我们只检查uRead.end<=checkStart的读。
        ReadLT uRead = unKnownReads.peek();//第一个元素，但不删除
        while (uRead!=null&&uRead.parent.end<=checkStart){
            unKnownReads.poll();
            long before = minReadTime.get(uRead.key).first();
            minReadTime.get(uRead.key).remove(uRead.parent.start);
            long nextMinRead = minReadTime.get(uRead.key).first();

            LinkedList<WriteLT> mayWriteList = writeMap.get(uRead.key);
            Iterator<WriteLT> iterator = mayWriteList.iterator();
            boolean find = false;
            while (iterator.hasNext()) {
                WriteLT mayWrite = iterator.next();
                if (mayWrite.opId == uRead.readFromWop){
                    if(mayWrite.replaceTime>uRead.parent.start) {
                        find = true;
                        under.addEdge(mayWrite.parent, uRead.parent, new DependencyEdge("WR"));
                    }
                    else
                        throw new ISException.ReadFromUnknown();
                }
                if (mayWrite.replaceTime <= nextMinRead) {
                    iterator.remove();//Remove
                    mayWrite.setState();
                } else if (mayWrite.parent.end <= uRead.parent.start) {
                    under.addEdge(mayWrite.parent, uRead.parent, new DependencyEdge("TO"));
                }
            }
            if(!find)
                throw new ISException.ReadFromUnknown();
            uRead = unKnownReads.peek();
        }
    }


















    public ArrayList<TransactionLT> getReads(DependencyGraph<TransactionLT,DependencyEdge> g, TransactionLT w){
        ArrayList<TransactionLT> readFrom = new ArrayList<>();
        Set<DependencyEdge> outgoingEdges =  g.outgoingEdgesOf(w);
        for(DependencyEdge e:outgoingEdges){
            if(Objects.equals(e.getLabel(), "WR"))
                readFrom.add(g.getEdgeTarget(e));
        }
        return readFrom;
    }






    private Set<DependencyEdge> tryAddEdge(DependencyGraph<TransactionLT, DependencyEdge> g, TransactionLT s, TransactionLT t, ArrayList<TransactionLT> reads){
        Set<DependencyEdge> conflictEdges;
        ;
        if((conflictEdges = g.checkEdge(s,t))!=null)
            return conflictEdges;

        else
            return null;
    }



    private void removeOldWrite(){
        for (Map.Entry<Long, LinkedList<WriteLT>> entry : writeMap.entrySet()) {
            long key = entry.getKey();
            LinkedList<WriteLT> wList = entry.getValue();

        }
    }

    public class SavaPoint{
        public boolean direction;
        public WritePair wwPair;
        public Set<WritePair> supportSet;
        public Set<WritePair> oppositionSet;
        public Set<WritePair> derivedSet;

        public SavaPoint(WritePair wwPair){
            this.wwPair = wwPair;
            this.derivedSet = new HashSet<>();
        }
        public void SetSupport(Set<WritePair> supportSet){
            this.supportSet = supportSet;
        }
        public void setOpposition(Set<WritePair> oppositionSet){
            this.oppositionSet = oppositionSet;
        }
        public void addDerivation(WritePair derived){
            this.derivedSet.add(derived);
        }
        public void backtrace(){

        }



    }


    private void wwPairDeduce(){
        for(WritePair wp:outdatedWritePairs){
            ArrayList<TransactionLT> readA = getReads(under,wp.txnA);
            ArrayList<TransactionLT> readB = getReads(under,wp.txnB);
            if(readA.isEmpty()&&readB.isEmpty())
                break;
            Set<DependencyEdge> conflictEdges;

            tryAddEdge(under,wp.txnA,wp.txnB,readA);//WW
            for(TransactionLT r :readA)
                tryAddEdge(under,r,wp.txnB);//RW






            //方向1
            try{
                under.addEdge(wp.txnA,wp.txnB);
                for(TransactionLT r :readA)
                    under.addEdge(r,wp.txnB);
            }catch (Exception e){
                //TODO:remove the Edge
                try{
                    under.addEdge(wp.txnA,wp.txnB);
                    for(TransactionLT r :readA)
                        under.addEdge(r,wp.txnB);
                }catch (Exception e){
                    //TODO:remove the Edge
                    //方向1，2都失败
                }
            }finally {

            }








        }
    }
    //
    private void removeObsoleteWrites(){
        Iterator<WriteLT> iterator = toObsoleteWrite.iterator();
        while (iterator.hasNext()){
            WriteLT w = iterator.next();
            if(w.replaceTime>checkStart)
                break;
            else{//replaceTime<=checkStart
                long minRead = minReadTime.get(w.key).first();
            }

        }

        WriteLT write = toObsoleteWrite.peek();
        while (write!=null&&write.replaceTime<=checkStart){
            toObsoleteWrite.poll();
            ArrayList<WritePair> wwList = wwPairs.get(write.key).get(write.txnId);
            Iterator<WritePair> iterator = wwList.iterator();
            while (iterator.hasNext()){
                WritePair wp = iterator.next();
                if
            }
        }

    }



    private void removeObsoleteWrite(){
        for (Map.Entry<Long, LinkedList<WriteLT>> entry : writeMap.entrySet()) {
            long key = entry.getKey();
            LinkedList<WriteLT> wList = entry.getValue();
            Iterator<WriteLT> iterator = wList.iterator();
            while (iterator.hasNext()){
                WriteLT write = iterator.next();
                if(write.replaceTime<=checkStart)//淘汰过时的写
                {
                    iterator.remove();
                    wwPairDeduce(write);
                }
            }
        }
    }


//    private boolean addEdge(DirectedAcyclicGraph<TransactionLT,DefaultEdge> g,TransactionLT s,TransactionLT t){
//        try {
//            g.addEdge(s,t);
//            return true;
//        }catch (GraphCycleProhibitedException e){
//            return false;
//        }
//    }
    //本地的更新添加到更新列表

    interface Observer {
        void update();
    }



    //本地写和已经存在的写的顺序关系
    private void sortWrite(HashMap<Long,WriteLT> localWrites){
        for(WriteLT local:localWrites.values()){
            long key = local.key;
            LinkedList<WriteLT> otherWrites = writeMap.get(key);
            if(otherWrites == null){
                LinkedList<WriteLT> newWrite = new LinkedList<>();
                newWrite.add(local);
                newWrite.add(Constants.initWrite);
                under.addEdge(Constants.initTxn,local.parent,new DependencyEdge("WW"));
                writeMap.put(key,newWrite);
            }
            else {
                for (WriteLT other : otherWrites) {
                    if (other.parent.end > checkStart) {
                        WritePair wwPair = new WritePair(local, other);//每个WWPair有两个极性，必须选择一个
                        local.wwPairs.add(wwPair);
                        other.wwPairs.add(wwPair);
                    } else {//other.parent.iEnd<=checkStart
                        //更新replaceTime
                        other.replaceTime = Math.min(checkEnd, other.replaceTime);
                        addEdge(under, other.parent, local.parent,"WW");//WW
                    }

                }
            }
        }
    }





    private void terminate(){

    }

}
=======
>>>>>>> refs/remotes/origin/main
