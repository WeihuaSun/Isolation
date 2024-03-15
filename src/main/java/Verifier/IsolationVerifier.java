//package Verifier;
//
//import benchmark.Config;
//
//import java.util.*;
//
//public class IsolationVerifier {
//    public final int SER = 4, SI = 3, RR = 2, RC = 1, RU = 0;
//    public ArrayList<ArrayList<Graph.Txn>> sessions;
//    public Graph cGraph;
//    public Graph.Txn[] sortedTxn;
//    public Graph.Txn initTxn;
//    public int ISLevel = 4;
//    public int RecordLevel = 1;
//    public ArrayList<Graph.Write> writes;
//    public HashMap<Long,LinkedList<Graph.Write>> writeMap; //key->writeList,按照iEndTime排序，大的在前面
//    public HashMap<Long,HashMap<Long,Graph.Write>> writeMapOId;//key->(opId->Write)
//    public PriorityQueue<Graph.Txn> aliveTxn = new PriorityQueue<>();
//    public Queue<Graph.Read> reads = new LinkedList<>();//
//    public PriorityQueue<Graph.Read> unknownReads = new PriorityQueue<>();
//    public IsolationVerifier(){
//        cGraph = new Graph();
//    }
//    public long curTime = 0;
//    public void initGraph(){
//        cGraph = new Graph();
//    }
////    public void processWrite(){
////        for (LinkedList<Graph.Write> wList:writeMap.values()){
////            for(int i=0;i<wList.size()-1;i++){
////                for(int j=1;j<wList.size();j++){
////
////                }
////            }
////        }
////    }
//    public void processUnknownReads() throws ISException.ReadFromUnknown {
//        Graph.Read r = null;
//        do {
//            r = unknownReads.poll();
//            Graph.Txn rfTxn = null;
//            if (r != null) {
//                ArrayList<Graph.Txn> wList = writeMap.get(r.key);//可能更新该Key的所有Write
//                if(wList == null)//如果该Key没有在可能的写列表里，则该Read读取的值要么是Aborted的事务更新的值，要么是不应该看到的旧值
//                    throw new ISException.ReadFromUnknown();
//                for (Graph.Txn w:wList){
//                    if (w.txnId == r.readFromTxn){//找到更新该Key的写操作所处的事务
//                        rfTxn = w;
//                        break;
//                    }
//                }
//                if (rfTxn == null)
//                    throw new ISException.ReadFromUnknown();
//                else //找到,在图中添加WR读写依赖
//                    cGraph.graph.addEdge(rfTxn,r.parent);
//            }
//
//        }while (r!=null&&r.snapshotEnd>curTime);
//    }
////    public void processRead() throws ISException.ReadFromUnknown {
////        while (true){
////            Graph.Read r = reads.poll();
////            Graph.Write rf = null;
////            if (r != null && r.end < curTime) {
////                //不能从Abort的事务读取和被覆盖的Write读取
////                //维护writeMap，保存历史中所有key当前可能的
////                ArrayList<Graph.Txn> wList = writeMap.get(r.key);
////                if(wList ==null)
////                    throw new ISException.ReadFromUnknown();
////                for (Graph.Write w:wList){
////                    if (w.txnId == r.txnId){
////                        rf = w;
////                        break;
////                    }
////                }
////                for (Graph.Write w:wList){
////                    cGraph.graph.addEdge(aliveTxn.)
////                }
////
////                if (rf == null)
////                    throw new ISException.ReadFromUnknown();
////
////
////
////            }
////            else
////                break;
////            }
////        }
//
//    static class WriteLine {
//        public long sStart, sEnd, iStart, iEnd;
//
//        public WriteLine(long sStart, long sEnd, long iStart, long iEnd) {
//            this.sStart = sStart;
//            this.sEnd = sEnd;
//            this.iStart = iStart;
//            this.iEnd = iEnd;
//        }
//    }
//
//    /**
//     *
//     * @param a write operation
//     * @param b write operation
//     * @return 1 if a ww-dependent b,-1 if b ww-dependent a,0 if a overlap with b
//     */
//    private int writeOverlap(WriteLine a,WriteLine b){
//        if(a.iStart<b.sEnd)
//            return 1;
//        else if(a.sEnd>b.iStart){
//            return -1;
//        }
//        return 0;
//    }
//
//    private void garbageCollect(){
//
//    }
//    private void findReadLevelOp(){
//
//    }
//    private void endLevelOp(){
//
//    }
//
////    private void sortWriteLevelOp(Graph.Txn txn, Collection<Graph.Write> localWrites) throws ISException.ConflictUpdate {
////
////    }
//    //Question:是否所有的环上都是确切的边？没有不确定的边！？
//    //
//    //写操作过时在此时应该调整
//    //如果某个写W1"确切"排在某个写W2前面，并且该curTime超过了W2.iEnd,那么W1已经过时了
//    //确切指的是RW,TO以及其推导出的WR,WW的传递闭包.
//    //对于写两个写相同Key的写操作W1,W2,有笔记中a,b,c三种推断方式
//    //--在curTime>max{W1.iEnd,W2.iEnd}后，后续可能会有R导致c的出现使得写排序，如果我们之前先假定一个顺序，如果和c推导的顺序一致
//    //则不改变，如果不一致，我们调整ww的顺序。
//    //--其中a,b在W1,W2都过时，或许可能通过第三方的写使得W-W依赖边成立
//    //
////    private void processAllWrites(){
////        for()
////    }
//    private void sortWriteLevelTxn(Graph.Txn txn, Collection<Graph.Write> localWrites){
//        long curTime = txn.iStart;
//        for(Graph.Write local:localWrites){
//            long key = local.key;
//            LinkedList<Graph.Write> otherWrites = writeMap.get(key);
//            if(otherWrites == null){//当前历史中没有更新该Key的写操作，则将该写操作和该Key绑定
//                LinkedList<Graph.Write> newWrites = new LinkedList<>();
//                newWrites.add(local);
//                writeMap.put(key,newWrites);
//            }
//            else{
//                //如果otherWrites的所有写都在local开始之前结束，那么local将代替全部的写
//                if(otherWrites.getFirst().parent.iEnd<txn.start){
//                    otherWrites.clear();
//                    otherWrites.add(local);
//                }
//
//
//
//                //历史中有更新该Key的写操作
//                //Note:对于Txn级别的记录，如果两个写操作事务重叠，并不能仅仅根据这两个事务进行排序
//                //我们可以根据三类路径a,b,c对两个事务进行排序，所以，我们要等到某个时间点，在这个时间点之后不会改变a,b,c路径的连通性
//                //1.如果到W1,W2都被替代，都没有出现读该Key的操作，则这两个写的排序已经无关紧要了。
//                //2.如果到W1,W2都被替代，出现读W1(W2)写的读操作，则该读操作对于另一个写的连通性显得至关重要。在curTime>max{r.iEnd,W.iEnd}后，不会再产生b,c类路径,如果b,c类路径没有出现，并且没有"未确定"边使得b,c类路径连通
//                //
//                //3.如果W1,W2所在的事务所有操作都被替代（过时），并且此时还没有出现a,b,c类边，则我们随机选择一条边
//            }
//        }
//    }
////    private void processReadLevelTxn(Graph.Read read){
////
////    }
////    private void sortWriteLevelEpoch(Graph.Txn txn, Collection<Graph.Write> localWrites){
////
////    }
////    private void checkReads(){
////        unknownReads.poll();
////        for(Graph.Read read:unknownReads){
////            if(read.parent.iEnd<=curTime)
////            {
////
////                for()
////            }
////
////        }
////    }
//    private void processTxnLevelOp(Graph.Txn curTxn) throws ISException.InternalRead, ISException.ReadFromUnknown, ISException.ConflictUpdate {
//        cGraph.under.addVertex(curTxn);
//        cGraph.over.addVertex(curTxn);
//        curTime = curTxn.start;
//        HashMap<Long, Graph.Write> localWrites = new HashMap<>();//key->WriteOp
//        for (Graph.TxnOp op : curTxn.txnOps) {
//            if (op instanceof Graph.Read) {
//                long key = ((Graph.Read) op).key;
//                long readFromTxn = ((Graph.Read) op).readFromTxn;
//                long readFromWop = ((Graph.Read) op).readFromWop;
//                Graph.Write mayWrite = localWrites.get(key);
//                if (mayWrite != null) {//curTxn更新过该Key
//                    if (mayWrite.opId != readFromWop) {//但是没有看到自己的更新
//                        throw new ISException.InternalRead(curTxn.txnId, key, readFromTxn);
//                    }
//                } else {//没有更新过该Key,则该值从其他事务读取,稍后处理
//                    unknownReads.add((Graph.Read) op);
//                }
//            } else if (op instanceof Graph.Write) {
//                localWrites.put(((Graph.Write) op).key, (Graph.Write) op);//更新记录,最后处理
//            }
//        }
//        //process Write:删除过时的Write
//        for(LinkedList<Graph.Write> wList:writeMap.values()) {
//           int pos = 0;
//            while (true)
//            {
//                Graph.Write tmpWrite = wList.get(pos);
//                if(tmpWrite.parent.iEnd<curTime){
//                    break;
//                }
//                pos++;
//            }
//            List<Graph.Write> sublistToDelete = wList.subList(pos + 1, wList.size());
//            sublistToDelete.clear();
//        }
//        //process Reads
//        //获取read.end<curTime的所有读
//        Graph.Read uRead = unknownReads.peek();
//        while (uRead!=null && uRead.end <= curTime) {
//            unknownReads.poll();//删除第一个元素
//            HashMap<Long, Graph.Write> mayWrite = writeMapOId.get(uRead.key);
//            LinkedList<Graph.Write> mayWriteList = writeMap.get(uRead.key);
//            if (uRead.readFromTxn == Config.initTxn){//从初始化读取
//                cGraph.under.addEdge(initTxn, uRead.parent);//添加WR边
//                if (mayWrite != null) {
//                    Graph.Write firstW = mayWriteList.getLast();//最早的
//                    if (uRead.start > firstW.parent.iEnd)//如果有更新该Key的操作，且Read在最早的操作结束之后才读的，应该读取该值，而不是Init
//                        throw new ISException.ReadFromUnknown();
//                    else
//                        cGraph.under.addEdge(uRead.parent, firstW.parent);//添加RW边
//                }
//            } else {//不从初始化读取
//                if (mayWrite == null) {
//                    throw new ISException.ReadFromUnknown();
//                } else {//mayWrite!=null
//                    Graph.Write rf = mayWrite.get(uRead.readFromTxn);
//                    if (rf == null)
//                        throw new ISException.ReadFromUnknown();
//                    else {//添加WR,RW边
//                        cGraph.under.addEdge(rf.parent, uRead.parent);//添加WR边
//                        cGraph.under.addEdge(uRead.parent, mayWriteList.get(mayWriteList.indexOf(rf) + 1).parent);//添加RW边
//                    }
//                }
//            }
//            uRead = unknownReads.peek();
//        }
//
//
//
//        //SortWrite
//        for(Graph.Write local:localWrites.values()){
//            long key = local.key;
//            LinkedList<Graph.Write> otherWrites = writeMap.get(key);
//            if(otherWrites == null){//当前历史中没有更新该Key的写操作，则将该写操作和该Key绑定
//                LinkedList<Graph.Write> newWrites = new LinkedList<>();
//                newWrites.add(local);
//                writeMap.put(key,newWrites);
//
//                HashMap<Long,Graph.Write> newWritesIds = new HashMap<>();
//                newWritesIds.put(local.txnId,local);
//                writeMapOId.put(local.key,newWritesIds);
//            }
//            else{//历史中有更新该Key的写操作
//                //Note:对于OP级别的记录，所有的写操作必然排序
//                //写操作的过时：
//                //1.后续的写不会与该写相交
//                //2.后续的读不会从该写读取
//                //对于OP级别的记录，如果curTime>Write.iEnd,那么该写已经过时了。
//                //我们按照推导出的写排序对写进行排序,最晚的在列表最前面，最早的在列表的最后面
//                WriteLine localLine = new WriteLine(local.sStart, local.sEnd, local.parent.iStart, local.parent.iEnd);
//                int pos = 0;
//                while (true){
//                    Graph.Write other = otherWrites.get(pos);
//                    if(curTime<other.parent.iEnd){//两个写有交集，需要判断两个写的顺序
//                        WriteLine otherLine = new WriteLine(other.sStart, other.sEnd, other.parent.iStart, other.parent.iEnd);
//                        int order = writeOverlap(otherLine,localLine);
//                        if(order == 0)//两个写无法区分，出现并行更新，出错
//                            throw new ISException.ConflictUpdate();
//                        else if (order == 1)//other>local
//                            pos++;
//                        else//order = -1,other<local
//                            break;
//                    }
//                }
//                otherWrites.add(pos,local);//将local添加到适当位置
//
//            }
//
//
//        }
//
//    }
////    public void processTxn(Graph.Txn curTxn) throws ISException.InternalRead {
////        cGraph.under.addVertex(curTxn);
////        cGraph.over.addVertex(curTxn);
////        HashMap<Long, Graph.Write> localWrites = new HashMap<>();//key->WriteOp
////        for (Graph.TxnOp op : curTxn.txnOps) {
////            if (op instanceof Graph.Read) {
////                long key = ((Graph.Read) op).key;
////                long readFromTxn = ((Graph.Read) op).readFromTxn;
////                long readFromWop = ((Graph.Read) op).readFromWop;
////                Graph.Write mayWrite = localWrites.get(key);
////                if (mayWrite != null) {//curTxn更新过该Key
////                    if (mayWrite.opId != readFromWop) {//但是没有看到自己的更新
////                        throw new ISException.InternalRead(curTxn.txnId, key, readFromTxn);
////                    }
////                } else {//没有更新过该Key,则该值从其他事务读取,稍后处理
////                    reads.add((Graph.Read) op);
////                }
////            } else if (op instanceof Graph.Write) {
////                localWrites.put(((Graph.Write) op).key, (Graph.Write) op);//更新记录,最后处理
////            }
////        }
////        //LocalWrites记录当前事务所做的最终更新，将其添加到WriteMap中
//////        long iStart = curTxn.commitReady;
//////        long iEnd = curTxn.commitDone;
////        switch (RecordLevel){
////            case 0://无间隔记录
////                break;
////            case 1://操作级别间隔记录
////                try {
////                    sortWriteLevelOp(curTxn,localWrites.values());
////                } catch (ISException.ConflictUpdate e) {
////                    throw new RuntimeException(e);
////                }
////                break;
////            case 2://事务级别间隔记录
////                sortWriteLevelTxn(curTxn,localWrites.values());
////                break;
////            case 3://段落级别间隔记录
////                sortWriteLevelEpoch(curTxn,localWrites.values());
////                break;
////            default:
////                //Error
////                break;
////        }
////
////        for(long key :localWrites.keySet()){
////            ArrayList<Graph.Txn> wList = writeMap.get(key);//更新该Key的写操作
////            if(wList!=null)//历史中有更新该Key的事务,则需要排序这些事务的顺序
////                for (Graph.Txn beforeWrite:wList){
////                    if(mw.commitDone<commitReady){
////
////                        cGraph.graph.addEdge(mw,curTxn);//添加WW依赖边
////                    }
////                }
////        }
////    }
////
////
////
////
////
////    public void feedTxn(Graph.Txn curTxn) throws ISException.InternalRead {
////        cGraph.under.addVertex(curTxn);
////        cGraph.over.addVertex(curTxn);
////
////        for (Graph.TxnOp op:curTxn.txnOps){
////            if (op instanceof Graph.Read) {
////                long key = ((Graph.Read) op).key;
////                long readFromTxn = ((Graph.Read) op).readFromTxn;
////                long readFromWop = ((Graph.Read) op).readFromWop;
////
////                //1.检查自己是否更新过该key值
////                Graph.Write mayWrite = localWrites.get(key);
////                if (mayWrite != null) {//curTxn更新过该Key
////                    if (mayWrite.opId != readFromWop) {//但是没有看到自己的更新
////                        throw new ISException.InternalRead(curTxn.txnId, key, readFromTxn);
////                    }
////                } else {//没有更新过该Key,则该值从其他事务读取,稍后处理
////                    reads.add((Graph.Read) op);
////                }
////            }
////            else if(op instanceof Graph.Write){
////                localWrites.put(((Graph.Write) op).key, (Graph.Write) op);//更新记录,最后处理
////            }
////        }
////
////        //首先识别可能的冲突。
////        //aliveTxn存储那些客户端结束时间晚于curTime的事务，按照结束时间从小到大排序。
////        try {
////            processRead();
////        } catch (ISException.ReadFromUnknown e) {
////            throw new RuntimeException(e);
////        }
////
////
////
////
////
////
////        //curTime是当前检查的事务的客户端开始时间，以后事务的客户端开始时间(实际开始时间)均晚于curTime.
////
////        //此时更新curTime,
////        //writesMap存储每个key对应的最新的写，有些写因为时间时间重叠，并不能区分谁是最新的。
////        //对于可串行化，我们要求可重复读。也就是说如果两个事务更新同一个Key,必然有
////
////        processWrite();
////
////
////    }
////
////    public boolean overlap(Object a,Object b){
////        long aStart=0,bStart=0;
////        long aEnd=0,bEnd=0;
////        if (a instanceof Graph.Txn){
////            aStart = ((Graph.Txn) a).start;
////            aEnd = ((Graph.Txn) a).end;
////        }
////        else if (a instanceof Graph.TxnOp){
////            aStart = ((Graph.TxnOp) a).start;
////            aEnd= ((Graph.TxnOp) a).end;
////        }
////        if (b instanceof Graph.Txn){
////            bStart = ((Graph.Txn) b).start;
////            bEnd = ((Graph.Txn) b).end;
////        }
////        else if (a instanceof Graph.TxnOp){
////            bStart = ((Graph.TxnOp) b).start;
////            bEnd= ((Graph.TxnOp) b).end;
////        }
////        if (aStart<bStart && aEnd>bStart)
////            return  true;
////        else return bStart < aStart && bEnd > aStart;
////
////
////    }
//    private void mergeSort(){
//        PriorityQueue<Element> minHeap = new PriorityQueue<>();
//        int resultSize = 0;
//        for(int i=0;i<sessions.size();i++){
//            ArrayList<Graph.Txn> session = sessions.get(i);
//            resultSize+=session.size();
//            if(!session.isEmpty())
//                minHeap.add(new Element(session.removeFirst(),i));
//        }
//        sortedTxn = new Graph.Txn[resultSize];
//        int index = 0;
//        while (!minHeap.isEmpty()){
//            Element element = minHeap.poll();
//            sortedTxn[index++]  = element.value;
//            feedTxn(element.value);
//            if(!sessions.get(element.rowIndex).isEmpty()){
//                minHeap.add(new Element(sessions.get(element.rowIndex).removeFirst(),element.rowIndex));
//            }
//        }
//    }
//    public static class Element implements Comparable<Element> {
//        Graph.Txn value;
//        int rowIndex;
//        public Element(Graph.Txn value, int rowIndex) {
//            this.value = value;
//            this.rowIndex = rowIndex;
//        }
//        @Override
//        public int compareTo(Element other) {
//            return Long.compare(this.value.start, other.value.start);
//        }
//    }
//
//    public class TxnEnd extends Graph.Txn implements Comparable<Graph.Txn>{
//        @Override
//        public int compareTo(Graph.Txn o) {
//            return Long.compare(this.start,o.start);
//        }
//
//        public TxnEnd(long txnId, long clientId) {
//            super(txnId, clientId);
//        }
//    }
//
//
//}
