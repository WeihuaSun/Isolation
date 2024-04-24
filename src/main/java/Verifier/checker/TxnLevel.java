package Verifier.checker;

import java.io.*;
import java.util.*;

import Verifier.writePair.WriteLT;
import Verifier.writePair.WritePair;
import graph.*;
import graph.edge.DependencyEdge;
import graph.edge.DependencyEdge.Type;
import graph.operator.AbortLT;
import graph.operator.OperatorLT;
import graph.operator.ReadLT;
import graph.vertex.TransactionLT;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.jgrapht.util.SupplierUtil;




public class TxnLevel extends Verifier {
    public Verifier.RunVerifier.IsolationLevel isolation;
    public DependencyGraph under;
    public DirectedWeightedMultigraph<TransactionLT, DependencyEdge> over;
    public TransactionLT curTxn;
    public long checkStart;
    public long checkEnd;
    public TreeSet<TransactionLT> aliveTxns;
    public PriorityQueue<ReadLT> unKnownReads;
    public HashMap<Long,TreeSet<Long>> minReadTime;
    public TreeSet<WriteLT> toObsoleteWrite;
    public HashMap<Long, TreeSet<WriteLT>> writeMap;
    public final TransactionLT initTransaction;

    public Set<WritePair> alivePairs;

    public TreeSet<WritePair> toProcessPairs;





    public TxnLevel(Verifier.RunVerifier.IsolationLevel isolation, String logPath){
        this.isolation = isolation;
        this.initTransaction = new TransactionLT();
        this.under = new DependencyGraph(null, SupplierUtil.createSupplier(DependencyEdge.class),true,false);
        this.over = new DirectedWeightedMultigraph<>(DependencyEdge.class);
        aliveTxns = new TreeSet<>((a,b)-> Math.toIntExact(a.end - b.end));
        unKnownReads = new PriorityQueue<>((a,b)-> Math.toIntExact(a.parent.end - b.parent.end));
        minReadTime = new HashMap<>();
        toObsoleteWrite = new TreeSet<>((a,b)-> Math.toIntExact(a.wReplaceTime - b.wReplaceTime));
        writeMap = new HashMap<>();
        alivePairs = new HashSet<>();
        toProcessPairs = new TreeSet<>((a,b)-> Math.toIntExact(a.checkTime - b.checkTime));
        List<List<TransactionLT>> history = new ArrayList<>();
        loadHistory(history,logPath);
        List<TransactionLT> sortedHistory = mergeSort(history);
        try {
            detectAnomaly(sortedHistory);
        }catch (Exception e){
            //TODO 异常报告
        }

    }

    private void loadHistory(List<List<TransactionLT>> history, String logPath) {
        File logDir = new File(logPath);
        int session = 0;
        for (File f : Objects.requireNonNull(logDir.listFiles())) {
            if (f.isFile() && f.getName().endsWith("log")) {
                try (FileInputStream fi = new FileInputStream(f)) {
                    BufferedInputStream bf = new BufferedInputStream(fi, 1024 * 1024);
                    DataInputStream in = new DataInputStream(bf);
                    ArrayList<TransactionLT> txns = new ArrayList<>();
                    TransactionLT txn = new TransactionLT();
                    char opType;
                    long time,txnId,opId,key,rfTxn,rfOp;
                    while (true) {
                        try {
                            opType = (char) in.readByte();
                        } catch (EOFException e) {
                            break;
                        }
                        switch (opType) {
                            case 'B'://begin
                                txnId = in.readLong();
                                time = in.readLong();
                                txn = new TransactionLT(txnId,session);
                                txn.setStart(time);
                                break;
                            case 'C'://commit
                                opId = in.readLong();
                                time = in.readLong();
                                txn.setEnd(time);
                                txn.appendOp(new CommitLT(opId,txn));
                                txns.addLast(txn);
                                break;
                            case 'A'://abort
                                opId = in.readLong();
                                time = in.readLong();
                                txn.setEnd(time);
                                txn.appendOp(new AbortLT(opId,txn));
                                //txns.addLast(txn);
                                break;
                            case 'W'://write
                                opId = in.readLong();
                                key = in.readLong();
                                txn.appendOp(new WriteLT(opId,txn,key));
                                break;
                            case 'R'://read
                                opId = in.readLong();
                                key = in.readLong();
                                rfTxn = in.readLong();
                                rfOp = in.readLong();
                                txn.appendOp(new ReadLT(opId,txn,key,rfTxn,rfOp));
                                break;
                            default:
                                break;
                        }
                    }
                    history.addLast(txns);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private List<TransactionLT> mergeSort(List<List<TransactionLT>> history){
        List<TransactionLT> sortedHistory = new ArrayList<>();
        PriorityQueue<Element> queue = new PriorityQueue<>((a,b)-> Math.toIntExact(a.value.start - b.value.start));
        for(int i=0;i<history.size();i++){
            if(!history.get(i).isEmpty()){
                queue.offer(new Element(i,0,history.get(i).getFirst()));
            }
        }
        while (!queue.isEmpty()){
            Element e = queue.poll();
            sortedHistory.add(e.value);
            int listIndex = e.listIndex;
            int nextIndex = e.index+1;
            if(nextIndex<history.get(listIndex).size()){
                queue.offer(new Element(listIndex,nextIndex,history.get(listIndex).get(nextIndex)));
            }
        }
        return sortedHistory;
    }

    static class Element{
        int listIndex;
        int index;
        TransactionLT value;
        Element(int listIndex,int index,TransactionLT value){
            this.listIndex = listIndex;
            this.index = index;
            this.value = value;
        }
    }

    public void detectAnomaly(List<TransactionLT> sortedHistory) throws Verifier.ISException.InternalRead, Verifier.ISException.ReadFromUnknown, Verifier.ISException.CycleException {
        for(TransactionLT txn:sortedHistory){
            under.addVertex(txn);
            over.addVertex(txn);
            checkStart = txn.start;
            checkEnd = txn.end;
            curTxn = txn;
            timeOrder();
            HashMap<Long,WriteLT> localWrites = analyseTransaction();
            removeObsoleteWrites();
            findReadFrom();
            sortWrite(localWrites);
            writeDeduce();
        }

    }
    public void timeOrder(){
        Iterator<TransactionLT> iterator = aliveTxns.iterator();
        while (iterator.hasNext()) {
            TransactionLT txn = iterator.next();
            if (txn.replaceTime <= checkStart)
                iterator.remove();
            else if (txn.end <= checkStart) {
                addEdge(under, txn, curTxn, Type.TO);
                addEdge(over, txn, curTxn,Type.TO);
                txn.replaceTime = Math.min(txn.replaceTime, checkEnd);
            }
        }
    }

    public void addEdge(DependencyGraph g, TransactionLT s, TransactionLT t, Type type, boolean ...d){
        DependencyEdge e = new DependencyEdge(type);
        if(d.length>0){
            e.setDeterminate(d[0]);
        }
        g.addEdge(s,t,e);
    }
    public void addEdge(DirectedWeightedMultigraph<TransactionLT,DependencyEdge> g,TransactionLT s,TransactionLT t,Type type,boolean ...d){
        DependencyEdge e = new DependencyEdge(type);
        if(d.length>0){
            e.setDeterminate(d[0]);
        }
        g.addEdge(s,t,e);
    }

    private HashMap<Long,WriteLT> analyseTransaction() throws Verifier.ISException.InternalRead {
        HashMap<Long, WriteLT> localWrites = new HashMap<>();//key->WriteOp
        for(OperatorLT op:curTxn.Ops){
            if(op instanceof ReadLT read){
                WriteLT mayWrite = localWrites.get(read.key);
                if(mayWrite !=null){
                    if (mayWrite.opId != read.readFromWop)
                        throw new Verifier.ISException.InternalRead();
                }
                else{
                    unKnownReads.add(read);
                    minReadTime.getOrDefault(read.key,new TreeSet<>()).add(read.parent.start);
                }
            }else if(op instanceof WriteLT write){
                localWrites.put(write.key,write);
            }
        }
        return localWrites;
    }

    /**
     * 寻找过时的写操作，过时的写操作指的是在以后的处理过程中，不会产生由该写直接产生的WR依赖边（不会有读操作读取该写的值）
     * toObsoleteWrite存储已经被替代但仍未过时的写操作，并按照replaceTime从小到大排序，使用TreeSet组成，因此可以动态的更新
     * 为了实现删除掉Write后不会再产生WR依赖，我们仔细的记录每个Key最早的仍未处理的读操作的开始时间minRead，如果write.replaceTime<=minRead，则该write可以安全删除
     * 如果当且没有minRead，则我们按照checkStart作为时间界限，即write.replaceTime<=checkStart，我们可以将其标记为过时，即从
     * 对于所有的Key来说，minRead是严格小于CheckStart的，所以当w.replaceTime>checkStart时，该还未完全过时，仍然有被读取的可能
     * Note:RW边的处理，当将边设置为过时时，需要添加RW边，寻找到所有读取该W的读，指向其所有后续的写
     */
    private void removeObsoleteWrites(){
        Iterator<WriteLT> iterator = toObsoleteWrite.iterator();
        while (iterator.hasNext()) {
            WriteLT w = iterator.next();
            if (w.replaceTime > checkStart) {
                break;
            } else {//replaceTime<=checkStart
                TreeSet<Long> readTime = minReadTime.getOrDefault(w.key, new TreeSet<>());
                if (readTime.isEmpty() || w.replaceTime <= readTime.first()) {
                    iterator.remove(); // 移除该WriteLT对象
                    w.setState();//调用自己的remove函数，去通知其所处的writePair
                    Set<TransactionLT> reads = getReads(under, w.parent);
                    for (TransactionLT r : reads) {
                        for(TransactionLT nw: w.neighbors){
                            addEdge(under, r, nw, Type.RW,true);//determinate
                            addEdge(over, r, nw, Type.RW,true);//determinate
                        }
                    }
                }
            }
        }
    }

    private Set<TransactionLT> getReads(DependencyGraph g, TransactionLT v){
        Set<TransactionLT> readFrom = new HashSet<>();
        Set<DependencyEdge> outgoingEdges = g.outgoingEdgesOf(v);
        for (DependencyEdge e : outgoingEdges) {
            if (e.isWR())
                readFrom.add(g.getEdgeTarget(e));
        }
        return readFrom;
    }

    //RW
    private void findReadFrom() throws Verifier.ISException.ReadFromUnknown {
        ReadLT uRead = unKnownReads.peek();//第一个元素，但不删除
        while (uRead != null && uRead.parent.end <= checkStart) {
            unKnownReads.poll();
            minReadTime.get(uRead.key).remove(uRead.parent.start);//更新minRead
            TreeSet<WriteLT> mayWriteList = writeMap.getOrDefault(uRead.key, new TreeSet<>((a, b) -> Math.toIntExact(a.parent.end - b.wReplaceTime)));
            Iterator<WriteLT> iterator = mayWriteList.iterator();
            boolean find = false;
            while (iterator.hasNext()) {
                WriteLT mayWrite = iterator.next();
                if (mayWrite.opId == uRead.readFromWop) {
                    if (mayWrite.replaceTime > uRead.parent.start) {
                        find = true;
                        addEdge(under, mayWrite.parent, uRead.parent, Type.WR);
                    } else
                        throw new Verifier.ISException.ReadFromUnknown();
                    break;
                }
            }
            if (!find)
                throw new Verifier.ISException.ReadFromUnknown();
            uRead = unKnownReads.peek();
        }

    }

    private TransactionLT getInitTransaction(){
        TransactionLT t = new TransactionLT();
        t.setStart(0);
        t.setEnd(0);
        return t;
    }

    private WriteLT getInitOperation(long replaceTime){
        WriteLT w = new WriteLT(-1,initTransaction,-1);
        w.setReplaceTime(replaceTime);
        return w;
    }
    /**
     * 处理当前事务的写操作cur，和之前事务写操作prev的关系，有两类关系:
     * a.prev.end<=cur.start:
     * 1.添加WW依赖关系，当且仅当该写在在另一个写结束后开始。该WW依赖关系是由TO推导出来的，所以需要将对应的TO的标签替换为WW
     * 2.更新ReplaceTime,如果cur.end比prev原有的ReplaceTime更早，那么将使用cur.end更新ReplaceTime
     * b.prev.end>cur.start，此时cur和prev时间戳相交，先后关系不确定，需要建立writePair，writePair过时后处理
     * 如果之前没有key相应的写操作，将cur作为该key的第二个写操作，第一个写操作为初始事务的写，initWrite
     *
     * @param localWrites 维护当前事务install的写操作，map:writeKey->WriteLT
     */
    private void sortWrite(HashMap<Long, WriteLT> localWrites){
        for (Map.Entry<Long, WriteLT> entry : localWrites.entrySet()) {
            long key = entry.getKey();
            WriteLT local = entry.getValue();
            TreeSet<WriteLT> otherWrites = writeMap.getOrDefault(key,new TreeSet<>((a,b)-> Math.toIntExact(a.wReplaceTime - b.wReplaceTime)));
            if(otherWrites.isEmpty()){
                WriteLT initOperation = getInitOperation(checkEnd);
                otherWrites.add(local);
                otherWrites.add(initOperation);
                initOperation.neighbors.add(local.parent);
                addEdge(under,initOperation.parent,local.parent,Type.WW,true);
                addEdge(over,initOperation.parent,local.parent,Type.WW,true);
            } else {
                for (WriteLT other : otherWrites) {
                    if (other.parent.end > checkStart) {
                        WritePair wwPair = new WritePair(local, other,alivePairs,toProcessPairs);//每个WWPair有两个极性，必须选择一个
                        alivePairs.add(wwPair);
                        local.wwPairs.add(wwPair);
                        other.wwPairs.add(wwPair);
                    } else {//other.parent.iEnd<=checkStart
                        //更新replaceTime
                        other.replaceTime = Math.min(checkEnd, other.replaceTime);
                        other.neighbors.add(local.parent);
                        addEdge(under, other.parent, local.parent, Type.WW,true);//WW
                        addEdge(over,other.parent,local.parent,Type.WW,true);
                    }
                }
            }
        }
    }

    /**
     * 推断wwPair依赖关系
     *
     * @throws Verifier.ISException.CycleException 出现环路异常
     */
    private void writeDeduce()throws Verifier.ISException.CycleException {
        for (WritePair wp : toProcessPairs) {
            wp.deduce(under);

//            List<List<DependencyEdge>> underSupportA = new ArrayList<>();
//            List<List<DependencyEdge>> underSupportB = new ArrayList<>();
//            boolean determinateA = wp.checkDirection(true, under, underSupportA);//try WriteB-> WriteA
//            boolean determinateB = wp.checkDirection(true, under, underSupportB);//try WriteA-> WriteB
//            if (determinateA && determinateB) {
//                /*
//                 如果A->B和B->A两个方向上都有确定的支撑路径，那么under中出现确定的循环，报错
//                 */
//                throw new ISException.CycleException();
//            } else if (determinateA) {//!determinateB
//                /*
//                A->B方向上有确定的支撑路径，B->A方向上没有确定的支撑路径
//                case1.B->A方向上有派生的支撑路径(!underSupportB.isEmpty())回溯B->A的派生支撑路径;
//                case2.B->A方向上没有派生的支撑路径(underSupportB.isEmpty())，则A->B变为确定的;
//                 */
//                if (!underSupportB.isEmpty()) {//case 1
//                    wp.backtrace(underSupportB);//回溯
//                } else {//case 2
//                    wp.setDirection(true, under);
//                    wp.setDirection(true, over);
//                }
//            } else if (determinateB) {//!determinateA
//                if (!underSupportA.isEmpty()) {
//                    wp.backtrace(underSupportA);//回溯
//                } else {
//                    wp.setDirection(false, under);
//                    wp.setDirection(false, over);
//                }
//            } else {//两个方向上都没有确定的支撑路径
//
//                if (underSupportA.isEmpty() && underSupportB.isEmpty()) {
//                    /*
//                    两个方向上都没有派生的支撑路径，则在over中检查
//                    case1:两个方向都有支撑，设置SavePoint
//                    case2:如果A->B有支撑，而B->A无支撑，则A->B是确定的
//                    case3:如果B->A有支撑，而A->B无支撑，则B->A是确定的
//                    case4.over中两个方向上都没有支撑边，则在以后的推导过程中A->B和B->A都不会参与环的形成
//                     */
//                    boolean overA = wp.checkDirection(false, over);//try writeB->writeA
//                    boolean overB = wp.checkDirection(true, over);//try writeA->writeB
//                    if (overA && overB) {//case 1
//                        /*
//                         * SavePoint:最自由的SavePoint,在over中两个方向上都有支撑集，如果我们选择方向A->B，其支撑集为Support(A->B),否定集为Support(B->A)
//                         * 在以后的执行过程中，当支撑集中的边变为确定的时候，我们删除该SavePoint，并将A->B设置为确认的，然后级联的操作
//                         * 在以后的执行过程中，当否定集中的路径变为确定的时候，需要回溯，幸运的是，我们并不回溯所有的，只回溯由该边引起的直接冲突和级联冲突
//                         * 否定集中，全部被否定，我们，我们也确定的推出A->B
//                         */
//                        wp.setDefaultDirection(under);
//                    } else if (overA) {//case 2
//                        wp.setDirection(true, under);
//                        wp.setDirection(true, over);
//                    } else if (overB) {//case 3
//                        wp.setDirection(false, under);
//                        wp.setDirection(false, over);
//                    } else {//case 4
//                        wp.clear();
//                    }
//                } else if (underSupportA.isEmpty()) {
//                    /*
//                    B->A有派生支撑路径，而A->B没有，检查over中是否存在A->B的支撑路径
//                    case1.如果存在A->B的支撑路径，则设置SavePoint
//                    case2.如果A->B无支撑路径，则B->A可以设置为确认
//                     */
//                    boolean overA = wp.checkDirection(false, over);
//                    if (overA) {
//                        /*
//                        savePoint
//                         */
//                        wp.setSavePoint(false,null);
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
//                        wp.setSavePoint(true,null);
//                    } else {
//                        wp.setDirection(true, under);
//                        wp.setDirection(true, over);
//                    }
//                } else {
//                    /*
//                    两个方向都有派生的支撑路径，则选择一个方向进行回溯
//                     */
//                    wp.defaultBacktrace(under);
//                }

            }
        }

    private void CycleReport() {

    }


}
