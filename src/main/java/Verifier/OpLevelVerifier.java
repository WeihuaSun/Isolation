package Verifier;

import Verifier.Exception.ISException;

import java.io.*;
import java.util.*;

public class OpLevelVerifier {
    public Graph DG;

    public HashMap<Long, LinkedList<Graph.Write>> writeMap = new HashMap<>();
    public PriorityQueue<Graph.Read> unknownReads = new PriorityQueue<>();//按照结束时间(OP level)排序
    public HashMap<Long,HashMap<Long,ArrayList<Graph.Read>>> readFrom = new HashMap<>();//key->(txnId->Read)
    public ArrayList<ArrayList<Graph.Txn>> sessions = new ArrayList<>();
    public Graph.Txn initTxn;
    public Graph.Write initWrite;
    public String logRoot = System.getProperty("user.dir")+"/output/ctwitter/";
    public int ISLevel = 4;
    public long curTime = 0;

    public OpLevelVerifier(){
        System.out.println("Verifier...");
        initTxn = new Graph.Txn(Graph.Txn.INIT_TXN,-1);
        initTxn.setStart(0,0);
        initTxn.setEnd(1,1);

        initWrite = new Graph.Write(Graph.Txn.INIT_OP, Graph.Txn.INIT_OP, 0L, 0L, 0L, 0L,0L,0L);
        initWrite.parent = initTxn;

        DG = new Graph();
        DG.under.addVertex(initTxn);
        loadHistory();
        mergeSort();
    }
    public void loadHistory(){
        File logDir = new File(logRoot);
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
                    sessions.add(extractLog(in,i));
                }
                i++;
            }
        } catch (IOException ignored) {
        }

    }
    public ArrayList<Graph.Txn> extractLog(DataInputStream in,int clientId) throws IOException{
        ArrayList<Graph.Txn> txns = new ArrayList<>();
        char opType;
        long txnId,opId,start,end,key,value,readFromTxn,readFromOp;
        Graph.Txn curTxn = new Graph.Txn(Graph.Txn.INIT_TXN,clientId);
        Graph.TxnOp op = null;
        boolean newTxn = true;
        while (true){
            try {
                opType = (char) in.readByte();
            }catch (EOFException e){
                break;
            }
            switch(opType) {
//                case 'S':
//                    txnId = in.readLong();
//                    opId = in.readLong();
//                    start = in.readLong();
//                    end = in.readLong();
//                    //curTxn = new Graph.Txn(txnId,clientId);
//                    op = new Graph.Begin(opId,txnId,start,end);
//                    break;
                case 'C':
                    txnId = in.readLong();
                    opId = in.readLong();
                    start = in.readLong();
                    end = in.readLong();
                    op = new Graph.Commit(opId,txnId,start,end);
                    curTxn.appendOp(op);
                    curTxn.setEnd(start,end);
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
                    op = new Graph.Abort(opId,txnId,start,end);
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
                        curTxn = new Graph.Txn(txnId,clientId);
                        curTxn.setStart(start,end);
                        newTxn = false;
                    }
                    if(curTxn.txnId!=txnId)
                        System.out.println("err");
                    if(ISLevel == 4)//
                        curTxn.appendOp(new Graph.Write(opId,txnId,start,end,key,value,start,end));
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
                        curTxn = new Graph.Txn(txnId,clientId);
                        curTxn.setStart(start,end);
                        newTxn = false;
                    }
                    if(curTxn.txnId!=txnId)
                        System.out.println("err");
                    if(ISLevel == 4)
                        curTxn.appendOp(new Graph.Read(opId,txnId,start,end,key,value,readFromTxn,readFromOp));
                    break;
                default:
                    break;
            }
        }
        return txns;
    }

    static class WriteLine {
        public long sStart, sEnd, iStart, iEnd;

        public WriteLine(long sStart, long sEnd, long iStart, long iEnd) {
            this.sStart = sStart;
            this.sEnd = sEnd;
            this.iStart = iStart;
            this.iEnd = iEnd;
        }
    }
    /**
     *
     * @param a write operation
     * @param b write operation
     * @return -1 if a ww-dependent b,1 if b ww-dependent a,0 if a overlap with b
     */
    private int writeOverlap(WriteLine a,WriteLine b){
        if(a.iStart<b.sEnd)
            return -1;//a<b
        else if(a.sEnd>b.iStart){
            return 1;//a>b
        }
        return 0;
    }

    private void end(){
        //TODO:
    }
    private void feedTxn(Graph.Txn curTxn) throws ISException.InternalRead, ISException.ReadFromUnknown, ISException.ConflictUpdate{
        DG.under.addVertex(curTxn);
        curTime = curTxn.sStart;
//        if(curTxn.txnId == 1489){
//            System.out.println("check txn");
//        }

        HashMap<Long, Graph.Write> localWrites = new HashMap<>();//key->WriteOp
        for (Graph.TxnOp op : curTxn.txnOps) {
            if (op instanceof Graph.Read) {
                long key = ((Graph.Read) op).key;
                long readFromTxn = ((Graph.Read) op).readFromTxn;
                long readFromWop = ((Graph.Read) op).readFromWop;
                Graph.Write mayWrite = localWrites.get(key);
                if (mayWrite != null) {//curTxn更新过该Key
                    if (mayWrite.opId != readFromWop) {//但是没有看到自己的更新
                        throw new ISException.InternalRead(curTxn.txnId, key, readFromTxn);
                    }
                } else {//没有更新过该Key,则该值从其他事务读取,稍后处理
                    unknownReads.add((Graph.Read) op);
                }
            } else if (op instanceof Graph.Write) {
                localWrites.put(((Graph.Write) op).key, (Graph.Write) op);//更新记录,最后处理
            }
        }





        //process Reads,WR依赖
        //获取read.end<curTime的所有读
        //应该检查W->R之间是否存在Wi,W->Wi &&Wi.iEnd<=R.sStart
        Graph.Read uRead = unknownReads.peek();
        while (uRead!=null && uRead.end <= curTime){
            unknownReads.poll();//删除第一个元素
            LinkedList<Graph.Write> mayWriteList = writeMap.get(uRead.key);
            if (uRead.readFromTxn == Graph.Txn.INIT_TXN){//从初始化读取
                DG.under.addEdge(initTxn, uRead.parent);//添加WR边
                if(!readFrom.containsKey(uRead.key))
                    readFrom.put(uRead.key,new HashMap<>());
                readFrom.get(uRead.key).put(initTxn.txnId,new ArrayList<>());
                readFrom.get(uRead.key).get(initTxn.txnId).add(uRead);//记录WR
//                if (mayWriteList != null) {
//                    Graph.Write firstW = mayWriteList.getLast();//最早的
//                    if (uRead.start > firstW.parent.iEnd)//如果有更新该Key的操作，且Read在最早的操作结束之后才读的，应该读取该值，而不是Init
//                        throw new ISException.ReadFromUnknown();
//                    else
//                        //一个事务读取后再更新，不添加RW依赖
//                        DG.under.addEdge(uRead.parent, firstW.parent);//添加RW边
//                }
            } else {//不从初始化读取
                if (mayWriteList == null) {
                    //System.out.println("read from unknown 1");
                    throw new ISException.ReadFromUnknown();
                } else {//mayWrite!=null
                    //找到对应的W
                    int pos = 0;
                    Graph.Write rf = null;
                    while (true)
                    {
                        Graph.Write mayWrite = mayWriteList.get(pos);
                        if(mayWrite.txnId == uRead.readFromTxn)
                        {
                            rf = mayWrite;
                            break;
                        }
                        pos++;
                        if(pos == mayWriteList.size())
                            break;
                    }
                    if (rf == null){
                        //System.out.println("read from unknown 2");
                        throw new ISException.ReadFromUnknown();
                    }

                    else {//添加WR,RW边?
                        DG.under.addEdge(rf.parent, uRead.parent);//添加WR边
                        readFrom.get(rf.key).get(rf.txnId).add(uRead);
                    }
                }
            }
            uRead = unknownReads.peek();
        }

        //process Write:删除过时的Write
        for (Map.Entry<Long, LinkedList<Graph.Write>> entry : writeMap.entrySet()) {
            //key = -9179727201708339
            long key = entry.getKey();

            LinkedList<Graph.Write> wList = entry.getValue();
            int oldLine = -1;
            for(int i = 0;i<wList.size();i++){
                if(wList.get(i).parent.iEnd<curTime){
                    //找到第一个iEnd小于curTime的写W_cur，该写的的前一个写W_prev已经过时
                    //1.以后出现的写都在W_cur后面，而不会出现在W_cur之前，所以不会产生W_prev->W_new的直接WW依赖
                    //2.以后出现的读都在W_cur后面，不会产生WR依赖于W_prev
                    //此时i的索引为W_cur
                    oldLine = i;
                    break;
                }
            }
            if(oldLine>=0&&oldLine<wList.size()-1) {
                //oldLine记录W_cur
                //1.oldLine>0表示找到W_cur
                //2.oldLine<wList.size()-1,表示oldLine至多为wList的倒数第二个元素，如果是倒数第一个元素，那么没有过时的W
                List<Graph.Write> sublistToDelete = wList.subList(oldLine+1, wList.size());

                //在删除掉过时的之前，我们添加WW依赖边和RW依赖边
                HashMap<Long, ArrayList<Graph.Read>> keyReads = readFrom.get(key);
                for(int pos = wList.size()-1;pos>oldLine;pos--){//从最早，写操作开始遍历

                    Graph.Write younger= wList.get(pos-1);
                    Graph.Write older  = wList.get(pos);
//                    if(key == 5955573542066334199L&& older.txnId == 2283) {
//                        System.out.println("check");//482
//                    }
                    ArrayList<Graph.Read> reads = keyReads.remove(older.txnId);//read from older
                    DG.under.addEdge(older.parent, younger.parent);//WW
                    if(reads!=null)
                        for (Graph.Read r : reads) {
                            //debug:有可能自己先读再更新，这样的RW不添加
                            if(r.parent == younger.parent)
                            {
                                keyReads.get(younger.txnId).add(r);//让其读自己，实际上依赖于younger的后一个写
                            }
                            else
                                DG.under.addEdge(r.parent, younger.parent);//RW
                        }
                }
                //删除掉过时的
                sublistToDelete.clear();
            }
        }

        //SortWrite:本地的写和已经出现的写的关系
        for (Graph.Write local : localWrites.values()) {
            long key = local.key;
            LinkedList<Graph.Write> otherWrites = writeMap.get(key);
            if (otherWrites == null) {//当前历史中没有更新该Key的写操作，则将该写操作和该Key绑定
                LinkedList<Graph.Write> newWrites = new LinkedList<>();
                newWrites.add(local);
                newWrites.add(initWrite);
                writeMap.put(key, newWrites);
                if(!readFrom.containsKey(key))
                    readFrom.put(key, new HashMap<>());
                //readFrom.get(key).put(initTxn.txnId, new ArrayList<>());//为写添加新的
                //DG.under.addEdge(initTxn, local.parent);//WW
            } else {//历史中有更新该Key的写操作
                //Note:对于OP级别的记录，所有的写操作必然排序
                //写操作的过时：
                //1.后续的写不会与该写相交
                //2.后续的读不会从该写读取
                //对于OP级别的记录，如果curTime>Write.iEnd,那么该写已经过时了。
                //我们按照推导出的写排序对写进行排序,最晚的在列表最前面，最早的在列表的最后面
                WriteLine localLine = new WriteLine(local.sStart, local.sEnd, local.parent.iStart, local.parent.iEnd);
                int pos = 0;
                while (true) {
                    Graph.Write other = otherWrites.get(pos);
                    WriteLine otherLine = new WriteLine(other.sStart, other.sEnd, other.parent.iStart, other.parent.iEnd);
                    int order = writeOverlap(otherLine, localLine);
                    if (order == 0)//两个写无法区分，出现并行更新，出错
                        throw new ISException.ConflictUpdate();
                    else if (order == 1)//other>local
                        pos++;
                    else//order = -1,other<local
                        break;
                }
                otherWrites.add(pos, local);//将local添加到适当位置
            }
            readFrom.get(key).put(local.txnId, new ArrayList<>());//为写添加新的
        }


    }
    private void mergeSort(){
        System.out.println("merge");
        PriorityQueue<Element> minHeap = new PriorityQueue<>();
        for(int i=0;i<sessions.size();i++){
            ArrayList<Graph.Txn> session = sessions.get(i);
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
            } catch (ISException.InternalRead | ISException.ReadFromUnknown | ISException.ConflictUpdate e) {
                //System.out.println(e.getMessage());
            }
            if(!sessions.get(element.rowIndex).isEmpty()){
                minHeap.add(new Element(sessions.get(element.rowIndex).removeFirst(),element.rowIndex));
            }
        }
        end();
    }

    public static class Element implements Comparable<Element> {
        Graph.Txn value;
        int rowIndex;
        public Element(Graph.Txn value, int rowIndex) {
            this.value = value;
            this.rowIndex = rowIndex;
        }
        @Override
        public int compareTo(Element other) {
            return Long.compare(this.value.sStart, other.value.sStart);
        }
    }



}
