package Verifier;

import java.io.*;
import java.util.*;

import Graph.*;
import Graph.DependencyEdge.Type;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.jgrapht.util.SupplierUtil;



public class TxnLevel extends Verifier{
    public RunVerifier.IsolationLevel isolation;
    public DependencyGraph under;
    public DirectedWeightedMultigraph<TransactionLT,DependencyEdge> over;
    public TransactionLT curTxn;
    public long checkStart;
    public long checkEnd;
    public TreeSet<TransactionLT> aliveTxns;

    public PriorityQueue<ReadLT> unKnownReads;

    public HashMap<Long,TreeSet<Long>> minReadTime;
    public TreeSet<WriteLT> toObsoleteWrite;
    public HashMap<Long, TreeSet<WriteLT>> writeMap;


    public TxnLevel(RunVerifier.IsolationLevel isolation,String logPath){
        this.isolation = isolation;
        this.under = new DependencyGraph(null, SupplierUtil.createSupplier(DependencyEdge.class),true,false);
        this.over = new DirectedWeightedMultigraph<>(DependencyEdge.class);
        aliveTxns = new TreeSet<>((a,b)-> Math.toIntExact(a.end - b.end));
        unKnownReads = new PriorityQueue<>((a,b)-> Math.toIntExact(a.parent.end - b.parent.end));
        minReadTime = new HashMap<>();
        toObsoleteWrite = new TreeSet<>((a,b)-> Math.toIntExact(a.wReplaceTime - b.wReplaceTime));
        writeMap = new HashMap<>();
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

    public void detectAnomaly(List<TransactionLT> sortedHistory) throws ISException.InternalRead, Verifier.ISException.ReadFromUnknown {
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

    private HashMap<Long,WriteLT> analyseTransaction() throws ISException.InternalRead {
        HashMap<Long, WriteLT> localWrites = new HashMap<>();//key->WriteOp
        for(OperatorLT op:curTxn.Ops){
            if(op instanceof ReadLT read){
                WriteLT mayWrite = localWrites.get(read.key);
                if(mayWrite !=null){
                    if (mayWrite.opId != read.readFromWop)
                        throw new ISException.InternalRead();
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
                    w.remove();//调用自己的remove函数，去通知其所处的writePair
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

    private Set<TransactionLT> getReads(DependencyGraph g,TransactionLT v){
        Set<TransactionLT> readFrom = new HashSet<>();
        Set<DependencyEdge> outgoingEdges = g.outgoingEdgesOf(v);
        for (DependencyEdge e : outgoingEdges) {
            if (e.isWR())
                readFrom.add(g.getEdgeTarget(e));
        }
        return readFrom;
    }

    private void findReadFrom() throws ISException.ReadFromUnknown {
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
                        throw new ISException.ReadFromUnknown();
                    break;
                }
            }
            if (!find)
                throw new ISException.ReadFromUnknown();
            uRead = unKnownReads.peek();
        }

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
                otherWrites.add(local);
                otherWrites.add(Constants.initWrite);
            }
            if (otherWrites == null) {
                LinkedList<WriteLT> newWrite = new LinkedList<>();
                newWrite.add(local);
                newWrite.add(Constants.initWrite);
                under.addEdge(Constants.initTxn, local.parent, new DependencyEdge(DependencyEdge.Type.WW));
                writeMap.put(key, newWrite);
            } else {
                for (WriteLT other : otherWrites) {
                    if (other.parent.end > checkStart) {
                        WritePair wwPair = new WritePair(local, other);//每个WWPair有两个极性，必须选择一个
                        local.wwPairs.add(wwPair);
                        other.wwPairs.add(wwPair);
                    } else {//other.parent.iEnd<=checkStart
                        //更新replaceTime
                        other.replaceTime = Math.min(checkEnd, other.replaceTime);
                        addEdge(under, other.parent, local.parent, "WW");//WW
                    }
                }
            }
        }

    }
    private void writeDeduce(){

    }
}
