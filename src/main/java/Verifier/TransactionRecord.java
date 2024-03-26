package Verifier;

import java.io.*;
import java.util.*;
import Graph.*;
import org.jgrapht.graph.*;
//TODO:Time Order

public class TxnLevelVerifier {
    public enum RecordLevel {Operator, Transaction,Group}
    public enum IsolationLevel {S_SER, SSI, SER, SI, RR, RC, RU}
    public int ISLevel;
    public ArrayList<ArrayList<TransactionLT>> history;//存储历史

    public HashMap<Long,TreeSet<Long>> minReadTime;//对于每个Key而言，还未处理的最早的读取时间，该时间决定了写的replaceTime;如果minReadTime为空，则参考值为当前的checkStart

    public DirectedWeightedMultigraph<TransactionLT,DependencyEdge> over;//有向多重图
    public DependencyGraph under;//DAG不允许出现循环

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

    public TxnLevelVerifier(){

        //public TxnLevelVerifier(String logDir,int ISLevel){
        this.ISLevel = ISLevel;
        this.logDir = logDir;
        unKnownReads = new PriorityQueue<>();
        //over = new DirectedMultigraph<>(DependencyEdge.class);
        under = new DependencyGraph(DependencyEdge.class);
        loadHistory();
        mergeSort();
    }

//
//    public void addEdge(AbstractBaseGraph g,TransactionLT s,TransactionLT t,String type){
//
//    }
//
//    /**
//     * 维护TimeOrder.主要是第三类边W->R
//     * @param nextTxn 按照开始时间排序的下一个要处理的事务
//     */
//    private void timeOrder(TransactionLT nextTxn){
//        Iterator<TransactionLT>iterator = aliveTxns.iterator();
//        while (iterator.hasNext()){
//            TransactionLT txn = iterator.next();
//            if(txn.replaceTime<=checkStart)
//                iterator.remove();
//            else if (txn.end<=checkStart) {
//                addEdge(under,txn,nextTxn,"TO");
//                addEdge(over,txn,nextTxn,"TO");
//                txn.replaceTime = Math.min(txn.replaceTime,checkEnd);
//            }
//        }
//    }
//
//    private void  loadHistory()
//    {
//        history = new ArrayList<>();
//        File logDir = new File(this.logDir);
//        ArrayList<File> logFiles = new ArrayList<>();
//        for (File f : Objects.requireNonNull(logDir.listFiles())) {
//            if (f.isFile() && f.getName().endsWith("log")) {
//                logFiles.add(f);
//            }
//        }
//        try {
//            int i = 0;
//            for (File f : logFiles) {
//                try (FileInputStream fi = new FileInputStream(f)) {
//                    BufferedInputStream bf = new BufferedInputStream(fi, 1024 * 1024);
//                    DataInputStream in = new DataInputStream(bf);
//                    history.add(extractLog(in,i));
//                }
//                i++;
//            }
//        } catch (IOException ignored) {
//        }
//    }
//    private ArrayList<TransactionLT> extractLog (DataInputStream in,int clientId)throws IOException{
//        ArrayList<TransactionLT> txns = new ArrayList<>();
//        char opType;
//        long txnId,opId,start,end,key,value,readFromTxn,readFromOp;
//        TransactionLT curTxn = new TransactionLT(TransactionLT.INIT_TXN,clientId);
//        boolean newTxn = true;
//        while (true){
//            try {
//                opType = (char) in.readByte();
//            }catch (EOFException e){
//                break;
//            }
//            switch(opType) {
//                case 'C':
//                    txnId = in.readLong();
//                    opId = in.readLong();
//                    start = in.readLong();
//                    end = in.readLong();
//
//                    curTxn.appendOp(new CommitLT(opId,curTxn));
//                    curTxn.setEnd(end);
//                    txns.add(curTxn);
//
//                    newTxn = true;
//                    if(curTxn.txnId!=txnId)
//                        break;
//                    break;
//                case 'A':
//                    System.out.println("ab");
//                    txnId = in.readLong();
//                    opId = in.readLong();
//                    start = in.readLong();
//                    end = in.readLong();
//                    //op = new Graph.Abort(opId,txnId,start,end);
//                    newTxn = true;
//                    break;
//                case 'W':
//                    txnId = in.readLong();
//                    opId = in.readLong();
//                    key = in.readLong();
//                    value = in.readLong();
//                    start = in.readLong();
//                    end = in.readLong();
//                    if(newTxn){
//                        curTxn = new TransactionLT(txnId,clientId);
//                        curTxn.setStart(start);
//                        newTxn = false;
//                    }
//                    if(curTxn.txnId!=txnId)
//                        System.out.println("err");
//                    curTxn.appendOp(new WriteLT(opId,curTxn,key));
//                    break;
//                case 'R':
//                    txnId = in.readLong();
//                    opId = in.readLong();
//                    key = in.readLong();
//                    value = in.readLong();
//                    start = in.readLong();
//                    end = in.readLong();
//                    readFromTxn = in.readLong();
//                    readFromOp = in.readLong();
//                    if(newTxn){
//                        curTxn = new TransactionLT(txnId,clientId);
//                        curTxn.setStart(start);
//                        newTxn = false;
//                    }
//                    if(curTxn.txnId!=txnId)
//                        System.out.println("err");
//                    curTxn.appendOp(new ReadLT(opId,curTxn,key,readFromTxn,readFromOp));
//                    break;
//                default:
//                    break;
//            }
//        }
//        return txns;
//    }
//
//
//
//    private void mergeSort(){
//        System.out.println("merge");
//        PriorityQueue<Element> minHeap = new PriorityQueue<>();
//        for(int i=0;i<history.size();i++){
//            ArrayList<TransactionLT> session = history.get(i);
//            if(!session.isEmpty())
//                minHeap.add(new Element(session.removeFirst(),i));
//        }
//        int i=0;
//        long now = System.currentTimeMillis();
//        while (!minHeap.isEmpty()){
//            Element element = minHeap.poll();
//            if(i%1000 == 0)
//                System.out.println(System.currentTimeMillis()-now);
//            //System.out.println(i);
//            i++;
//            try {
//                feedTxn(element.value);
//            } catch (ISException.InternalRead | ISException.ReadFromUnknown |
//                     ISException.CycleException e) {
//                //System.out.println(e.getMessage());
//            }
//            if(!history.get(element.rowIndex).isEmpty()){
//                minHeap.add(new Element(history.get(element.rowIndex).removeFirst(),element.rowIndex));
//            }
//        }
//        terminate();
//    }
//
//    public static class Element implements Comparable<OpLevelVerifier.Element> {
//        TransactionLT value;
//        int rowIndex;
//        public Element(TransactionLT value, int rowIndex) {
//            this.value = value;
//            this.rowIndex = rowIndex;
//        }
//        @Override
//        public int compareTo(OpLevelVerifier.Element other) {
//            return Long.compare(this.value.start, other.value.start);
//        }
//    }
//    private void feedTxn(TransactionLT curTxn) throws ISException.InternalRead, ISException.ReadFromUnknown, ISException.CycleException {
//        under.addVertex(curTxn);
//        over.addVertex(curTxn);
//        checkStart = curTxn.start;
//        checkEnd = curTxn.end;
//        timeOrder(curTxn);//处理TimeOrder
//        HashMap<Long, WriteLT> txnWrites = analyseTransaction(curTxn);//done
//        //处理写
//        sortWrite(txnWrites);
//        //处理读
//        findReadFrom();
//        //处理过时
//        removeObsoleteWrites();
//
//
//    }
//
//    private HashMap<Long,WriteLT> analyseTransaction(TransactionLT txn) throws ISException.InternalRead {
//        HashMap<Long, WriteLT> localWrites = new HashMap<>();//key->WriteOp
//        for(OperatorLT op:txn.Ops){
//            if(op instanceof ReadLT read){
//                WriteLT mayWrite = localWrites.get(read.key);
//                if(mayWrite !=null){
//                    if (mayWrite.opId != read.readFromWop)
//                        throw new ISException.InternalRead();
//                }
//                else{
//                    unKnownReads.add(read);
//                    minReadTime.get(read.key).add(read.parent.start);
//                }
//            }else if(op instanceof WriteLT write){
//                localWrites.put(write.key,write);
//            }
//        }
//        return localWrites;
//    }
//
//
//    //private void addEdge(DependencyGraph g, TransactionLT s, TransactionLT t,String type){
////        try {
////            g.addEdge(s,t,new DependencyEdge(type));
////        }catch (GraphCycleProhibitedException e){
////
////        }
////    }
//
//
//
//    private void findReadFrom() throws ISException.ReadFromUnknown, ISException.CycleException {
//        //unKnownReads按照结束时间安排队列，结束时间最早的在队列前，我们只检查uRead.end<=checkStart的读。
//        ReadLT uRead = unKnownReads.peek();//第一个元素，但不删除
//        while (uRead != null && uRead.parent.end <= checkStart) {
//            unKnownReads.poll();
//            long before = minReadTime.get(uRead.key).first();
//            minReadTime.get(uRead.key).remove(uRead.parent.start);
//            long nextMinRead = minReadTime.get(uRead.key).first();
//
//            LinkedList<WriteLT> mayWriteList = writeMap.get(uRead.key);
//            Iterator<WriteLT> iterator = mayWriteList.iterator();
//            boolean find = false;
//            while (iterator.hasNext()) {
//                WriteLT mayWrite = iterator.next();
//                if (mayWrite.opId == uRead.readFromWop) {
//                    if (mayWrite.replaceTime > uRead.parent.start) {
//                        find = true;
//                        under.addEdge(mayWrite.parent, uRead.parent, new DependencyEdge(DependencyEdge.Type.WR));
//                    } else
//                        throw new ISException.ReadFromUnknown();
//                }
//                if (mayWrite.replaceTime <= nextMinRead) {
//                    iterator.remove();//Remove
//                    mayWrite.setState();
////                } else if (mayWrite.parent.end <= uRead.parent.start) {
////                    under.addEdge(mayWrite.parent, uRead.parent, new DependencyEdge("TO"));
////                }
//                }
//                if (!find)
//                    throw new ISException.ReadFromUnknown();
//                uRead = unKnownReads.peek();
//            }
//        }}
//
//
////    public ArrayList<TransactionLT> getReads(DependencyGraph g, TransactionLT w) {
////        ArrayList<TransactionLT> readFrom = new ArrayList<>();
////        Set<DependencyEdge> outgoingEdges = g.outgoingEdgesOf(w);
////        for (DependencyEdge e : outgoingEdges) {
////            if (Objects.equals(e.getLabel(), "WR"))
////                readFrom.add(g.getEdgeTarget(e));
////        }
////        return readFrom;
////    }
//
//
//    private Set<DependencyEdge> tryAddEdge(DependencyGraph g, TransactionLT
//            s, TransactionLT t, ArrayList<TransactionLT> reads) {
//        Set<DependencyEdge> conflictEdges;
////        if ((conflictEdges = g.checkEdge(s, t)) != null)
////            return conflictEdges;
//
////        else
//            return null;
//    }
//
//
//    private void removeOldWrite() {
//        for (Map.Entry<Long, LinkedList<WriteLT>> entry : writeMap.entrySet()) {
//            long key = entry.getKey();
//            LinkedList<WriteLT> wList = entry.getValue();
//
//        }
//    }
//
//
//    //TODO: 推导的边的范围是多少？:
//    public class SavaPoint {
//        //SavePoint
//        //1.wwPair在under两个方向上都没有支撑集
//        //2.wwPair在over两个方向上都存在支撑集
//        public WritePair wwPair;//ww方向及其对应的RW边
//        public Set<WritePair> supportSet;
//        public Set<WritePair> oppositionSet;
//        public Set<WritePair> derivedSet;
//
//        public SavaPoint(WritePair wwPair) {
//            this.wwPair = wwPair;
//            this.derivedSet = new HashSet<>();
//        }
//
//        public void SetSupport(Set<WritePair> supportSet) {
//            this.supportSet = supportSet;
//        }
//
//        public void setOpposition(Set<WritePair> oppositionSet) {
//            this.oppositionSet = oppositionSet;
//        }
//
//        public void addDerivation(WritePair derived) {
//            this.derivedSet.add(derived);
//        }
//
//        public void backtrace() {
//            //最小量的回溯？or完全回溯
//            //如果derivedSet导致出现了环路，并且
//
//        }
//
//
//    }
//
//
//    private boolean back() {
//        return true;
//    }
//
//    public void cycleReport() throws ISException.CycleException {
//        throw new ISException.CycleException();
//    }
//
//    public void setDirection(){
//
//    }
//    public void addSupport(){
//
//    }
//
//    /**
//     * 依赖图的最小割
//     * @param g
//     * @return
//     */
//    public List<DependencyEdge> minCut(Object g){
//        return null;
//    }
//    public boolean backtrace(List<List<DependencyEdge>> derivedPaths){
//        List<DependencyEdge> cut = minCut(derivedPaths);
//        if(cut.isEmpty()){//没有最小割，
//
//        }
//        for(DependencyEdge e:cut){
//            for(WritePair wp:e.writePairs){
//                wp.backtrace();
//            }
//        }
//        return true;
//    }
//
//    /**
//     * 推断wwPair依赖关系
//     *
//     * @throws ISException.CycleException 出现环路异常
//     */
//    private void wwPairDeduce() throws ISException.CycleException {
//        //Question:在使用savepoint推导后，是否需要在over中删除和添加某些边？
//        //Note：否定集的否定是直接的推出，推出集包含否定集合
//        for (WritePair wp : outdatedWritePairs) {
//            List<List<DependencyEdge>> underSupportA = new ArrayList<>();
//            List<List<DependencyEdge>> underSupportB = new ArrayList<>();
//            boolean determinateA = wp.checkDirection(true, under, underSupportA);//try WriteB-> WriteA
//            boolean determinateB = wp.checkDirection(true, under, underSupportB);//try WriteA-> WriteB
//
//            if (determinateA && determinateB) {
//                /*
//                 如果A->B和B->A两个方向上都有确定的支持路径，那么under中出现确定的循环，报错
//                 */
//                cycleReport();
//            } else if (determinateA) {
//                /*
//                A->B方向上有确定的支撑路径，B->A方向上没有确定的支撑路径
//                case1.B->A方向上有不确定的支撑路径(!underSupportB.isEmpty())回溯B->A的支撑路径;
//                case2.B->A方向上没有不确定的支撑路径(underSupportB.isEmpty())，则A->B变为确定的
//                 */
//                //case 1
//                if (!underSupportB.isEmpty()) {
//                    backtrace(underSupportB);
//                } else {
//                    wp.setDirection(true, under);
//                    wp.setDirection(true, over);
//                }
//            } else if (determinateB) {
//                if (!underSupportA.isEmpty()) {
//                    wp.backtrace();
//                } else {
//                    wp.setDirection(false, under);
//                    wp.setDirection(false, over);
//                }
//            } else {
//                /*
//                两个方向上都没有确定的支撑边
//                 */
//                if (underSupportA.isEmpty() && underSupportB.isEmpty()) {
//                    /*
//                    两个方向上都没有派生的支撑边，则在over中检查
//                    case1.如果over中两个方向上都没有支撑边，则在以后的推导过程中A->B和B->A都不会参与环的形成
//                    case2:如果A->B有支撑，而B->A无支撑，则A->B是确定的
//                    case3:B->A
//                    case4:两个方向都有支撑，设置SavePoint
//                     */
//                    boolean overA = wp.checkDirection(false, over);
//                    boolean overB = wp.checkDirection(true, over);
//                    if (overA && overB) {
//                        /*
//                         * SavePoint:最自由的SavePoint,在over中两个方向上都有支撑集，如果我们选择方向A->B，其支撑集为Support(A->B),否定集为Support(B->A)
//                         * 在以后的执行过程中，当支撑集中的边变为确定的时候，我们删除该SavePoint，并将A->B设置为确认的，然后级联的操作
//                         * 在以后的执行过程中，当否定集中的路径变为确定的时候，需要回溯，幸运的是，我们并不回溯所有的，只回溯由该边引起的直接冲突和级联冲突
//                         * 否定集中，全部被否定，我们，我们也确定的推出A->B
//                         */
//                    } else if (overA) {//A->B有支撑集，而B->A没有
//                        wp.setDirection(true, under);
//                        wp.setDirection(true, over);
//                    } else if (overB) {
//                        wp.setDirection(false, under);
//                        wp.setDirection(false, over);
//                    } else {
//                        wp.clear();
//                    }
//                } else if (underSupportA.isEmpty()) {
//                    /*
//                    B->A有支撑集，而A->B没有，检查over中是否存在A->B的支撑集
//                    case1.如果存在A->B的支撑，则设置SavePoint
//                    case2.如果A->B无支持，则B->A可以设置为确认
//                     */
//                    boolean overA = wp.checkDirection(false, over);
//                    if (overA) {
//                        /*
//                        savePoint
//                         */
//                    } else {
//                        wp.setDirection(false, under);
//                        wp.setDirection(false, over);
//                    }
//                } else if (underSupportB.isEmpty()) {
//                    boolean overB = wp.checkDirection(true, over);
//                    if (overB) {
//                        /*
//                        savePoint
//                         */
//                    } else {
//                        wp.setDirection(true, under);
//                        wp.setDirection(true, over);
//                    }
//                } else {
//                    /*
//                    两个方向都有派生的支撑边，则选择一个方向进行回溯
//                     */
//                }
//
//            }
//        }
//    }
//
//
//    /**
//     * 寻找过时的写操作，过时的写操作指的是在以后的处理过程中，不会产生由该写直接产生的WR依赖边（不会有读操作读取该写的值）
//     * toObsoleteWrite存储已经被替代但仍未过时的写操作，并按照replaceTime从小到大排序，使用TreeSet组成，因此可以动态的更新
//     * 为了实现删除掉Write后不会再产生WR依赖，我们仔细的记录每个Key最早的仍未处理的读操作的开始时间minRead，如果write.replaceTime<=minRead，则该write可以安全删除
//     * 如果当且没有minRead，则我们按照checkStart作为时间界限，即write.replaceTime<=checkStart，我们可以将其标记为过时，即从
//     * 对于所有的Key来说，minRead是严格小于CheckStart的，所以当w.replaceTime>checkStart时，该还未完全过时，仍然有被读取的可能
//     * Note:RW边的处理，当将边设置为过时时，需要添加RW边，寻找到所有读取该W的读，指向其所有后续的写
//     */
//    private void removeObsoleteWrites() {
//        Iterator<WriteLT> iterator = toObsoleteWrite.iterator();
//        while (iterator.hasNext()) {
//            WriteLT w = iterator.next();
//            if (w.replaceTime > checkStart) {
//                break;
//            } else {//replaceTime<=checkStart
//                TreeSet<Long> readTime = minReadTime.getOrDefault(w.key, new TreeSet<>());
//                if (readTime.isEmpty() || w.replaceTime <= readTime.first()) {
//                    iterator.remove(); // 移除该WriteLT对象
//                    Set<TransactionLT> reads = getReads(under, w.parent);
//                    for (TransactionLT r : reads) {
//                        addEdge(under, r, w.parent, "RW");
//                        addEdge(over, r, w.parent, "RW");
//                    }
//                }
//            }
//        }
//    }
//
//
//    public Set<TransactionLT> getReads(DependencyGraph g, TransactionLT w) {
//        Set<TransactionLT> readFrom = new HashSet<>();
//        Set<DependencyEdge> outgoingEdges = g.outgoingEdgesOf(w);
//        for (DependencyEdge e : outgoingEdges) {
//            if (e.isWR())
//                readFrom.add(g.getEdgeTarget(e));
//        }
//        return readFrom;
//    }
//
//
//
//
////    private boolean addEdge(DirectedAcyclicGraph<TransactionLT,DefaultEdge> g,TransactionLT s,TransactionLT t){
////        try {
////            g.addEdge(s,t);
////            return true;
////        }catch (GraphCycleProhibitedException e){
////            return false;
////        }
////    }
//    //本地的更新添加到更新列表
//
//    /**
//     *处理当前事务的写操作cur，和之前事务写操作prev的关系，有两类关系:
//     * a.prev.end<=cur.start:
//     *      1.添加WW依赖关系，当且仅当该写在在另一个写结束后开始。该WW依赖关系是由TO推导出来的，所以需要将对应的TO的标签替换为WW
//     *      2.更新ReplaceTime,如果cur.end比prev原有的ReplaceTime更早，那么将使用cur.end更新ReplaceTime
//     * b.prev.end>cur.start，此时cur和prev时间戳相交，先后关系不确定，需要建立writePair，writePair过时后处理
//     * 如果之前没有key相应的写操作，将cur作为该key的第二个写操作，第一个写操作为初始事务的写，initWrite
//     * @param localWrites 维护当前事务install的写操作，map:writeKey->WriteLT
//     */
//    private void sortWrite(HashMap<Long,WriteLT> localWrites){
//        for(Map.Entry<Long,WriteLT> entry:localWrites.entrySet()){
//            long key = entry.getKey();
//            WriteLT local = entry.getValue();
//            LinkedList<WriteLT> otherWrites = writeMap.get(key);
//            if(otherWrites == null){
//                LinkedList<WriteLT> newWrite = new LinkedList<>();
//                newWrite.add(local);
//                newWrite.add(Constants.initWrite);
//                under.addEdge(Constants.initTxn,local.parent,new DependencyEdge(DependencyEdge.Type.WW));
//                writeMap.put(key,newWrite);
//            }
//            else {
//                for (WriteLT other : otherWrites) {
//                    if (other.parent.end > checkStart) {
//                        WritePair wwPair = new WritePair(local, other);//每个WWPair有两个极性，必须选择一个
//                        local.wwPairs.add(wwPair);
//                        other.wwPairs.add(wwPair);
//                    } else {//other.parent.iEnd<=checkStart
//                        //更新replaceTime
//                        other.replaceTime = Math.min(checkEnd, other.replaceTime);
//                        addEdge(under, other.parent, local.parent,"WW");//WW
//                    }
//                }
//            }
//        }
//    }
//
//
//
//
//
//    private void terminate(){
//
//    }

}
