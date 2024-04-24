package Graph;

import Verifier.ISException;
import org.jgrapht.graph.*;

import java.util.*;

public class WritePair {

    private final Object directionObj;
    public WriteLT writeA;
    public WriteLT writeB;
    private int stage;//state:0-两个write都未过时；1-一个write过时；2-两个write过时
    public boolean direction;//true:A->B,false:B->A
    public long checkTime;
    private Set<TransactionLT> readFromA;//读取A的写所在的事务集合
    private Set<TransactionLT> readFromB;//读取B的写所在的事务集合
    public Set<WritePair> alive;//指向主函数
    public TreeSet<WritePair> toProcess;//指向主函数
    public boolean isSavePoint;
    private List<SupportGraph> underSupportTrue;
    private List<SupportGraph> underSupportFalse;
    private List<SupportGraph> overSupportTrue;
    private List<SupportGraph> overSupportFalse;
    boolean assigned = false;




    public WritePair(WriteLT writeA, WriteLT writeB,Set<WritePair> alive,TreeSet<WritePair> toProcess) {
        this.writeA = writeA;
        this.writeB = writeB;
        this.stage = 0;
        this.readFromA = null;
        this.readFromB = null;
        this.alive = alive;
        this.toProcess = toProcess;
        this.isSavePoint = false;
        this.directionObj = new Direction();
    }

    public void deduce(DependencyGraph g) throws ISException.CycleException {
        boolean supportTrue = checkDirection(true,g);
        boolean supportFalse = checkDirection(false,g);
        if (supportTrue && supportFalse) {
            /* 如果A->B和B->A两个方向上都有确定的支撑路径，那么出现确定的循环，抛出异常 */
            throw new ISException.CycleException();
        } else if(supportTrue){
            /*
            A->B方向上有确定的支撑路径，B->A方向上没有确定的支撑路径
            case1.B->A方向上有派生的支撑路径(!underSupportFalse.isEmpty())，则回溯B->A的派生支撑路径;
            case2.B->A方向上没有派生的支撑路径(underSupportFalse.isEmpty())，则A->B变为确定的;
            */
            if (!underSupportFalse.isEmpty()) {//case 1
                backTrace();
            } else {//case 2
                setDirection(true, g);
            }
        } else if (supportFalse) {
            if (!underSupportTrue.isEmpty()) {//case 1
                backTrace();
            } else {//case 2
                setDirection(false, g);
            }
        }else {
            if(underSupportTrue.isEmpty()&&underSupportFalse.isEmpty()){
                if(!overSupportTrue.isEmpty()&&!overSupportFalse.isEmpty()){
                    boolean direction = writeA.parent.end<=writeB.parent.end;
                    setSavePoint(direction,g);
                } else if (!overSupportTrue.isEmpty()) {
                    setDirection(true, g);
                } else if (!overSupportFalse.isEmpty()) {
                    setDirection(false, g);
                } else {
                    clear();
                }
            }else if(underSupportTrue.isEmpty()){
                if(!overSupportTrue.isEmpty()){
                    setSavePoint(false,g);
                }else{
                    setDirection(false,g);
                }
            }else if(underSupportFalse.isEmpty()){
                if(!overSupportFalse.isEmpty()){
                    setSavePoint(true,g);
                }
                else{
                    setDirection(true,g);
                }
            }else{
                boolean direction = writeA.parent.end>=writeB.parent.end;
                backTrace(direction);
            }
        }
    }
    private void backTrace(boolean direction,boolean determinate,DependencyGraph g){
        List<SupportGraph> supports;
        if (direction)
            supports = underSupportTrue;
        else
            supports = underSupportFalse;
        for(SupportGraph g:supports){
            HashMap<DependencyEdge,Boolean> cut = g.getCut();
            for(Map.Entry<DependencyEdge,Boolean> entry: cut.entrySet()){
                entry.getKey().BackTrace(entry.getValue()||determinate);
            }
        }
        if(assigned)
            reverseDirection(determinate,g);

    }
    private void reverseDirection(boolean determinate,DependencyGraph g){

    }

    public class Direction {


    }


    private void backTrace(){
        backTrace(this.direction);
    }
    private boolean checkDirection(boolean direction,DependencyGraph g){
        List<SupportGraph> supportOvers = new ArrayList<>();
        List<SupportGraph> supportUnders = new ArrayList<>();
        TransactionLT from,to;
        Set<TransactionLT> readFrom;
        if(direction){//writeA->writeB
            from = writeA.parent;
            to = writeB.parent;
            readFrom =  getReadFromA(g);
        }
        else{//writeB->writeA
            from = writeB.parent;
            to = writeA.parent;
            readFrom = getReadFromB(g);
        }
        SupportGraph over = null;
        SupportGraph under = null;
        if(g.checkEdge(from,to,over,under))//try WW
            if(over!=null)
                supportOvers.add(over);
            if(under!=null)
                supportUnders.add(under);
            return true;
        for(TransactionLT read:readFrom){

            if(g.checkEdge(read,to,derivedPaths))//try RW
                return true;
        }
        return false;


    }
    public void clear(){
        this.toProcess.remove(this);
    }
    public void setSavePoint(boolean direction,Object subGraph){

    }
    public void defaultSavePoint(){

    }
    public void defaultBacktrace(DependencyGraph g){

    }

    /**
     * 找到图的一个割，进行回溯
     * @param subGraph A->B涉及到的子图
     * @throws ISException.CycleException 找不到一个割，无法回溯
     */
    public void backtrace(Object subGraph) throws ISException.CycleException {

    }
    public Set<TransactionLT> getReadFromA(DependencyGraph g) {
        if(this.readFromA==null)
            this.readFromA = getReads(g,writeA.parent);
        return this.readFromA;
    }
    public Set<TransactionLT> getReadFromB(DependencyGraph g){
        if(this.readFromB==null)
            this.readFromB = getReads(g,writeB.parent);
        return this.readFromB;
    }

    public void setDirection(boolean direction, DependencyGraph g) {
        this.direction = direction;
        TransactionLT from,to;
        Set<TransactionLT> readFrom;
        if (direction) {//writeA->writeB
            readFrom = getReadFromA(g);
            from = writeA.parent;
            to = writeB.parent;
        } else {//writeB->writeA
            from = writeB.parent;
            to = writeA.parent;
            readFrom = getReadFromB(g);
        }
        addEdge(g,from,to, DependencyEdge.Type.WW);
        for(TransactionLT read :readFrom){
            addEdge(g,read,to,DependencyEdge.Type.RW);
        }
    }

    public void setDefaultDirection(DependencyGraph g){

    }
    public void addEdge(DependencyGraph g,TransactionLT sourceVertex, TransactionLT targetVertex,DependencyEdge.Type type){

            DependencyEdge edge = g.getEdge(sourceVertex,targetVertex);
            if(edge!=null){
                edge.setType(type);
            }
            else{
                g.addEdge(sourceVertex,targetVertex,new DependencyEdge(type));
            }

    }

    public void setDirection(boolean direction,DirectedWeightedMultigraph<TransactionLT,DependencyEdge> g){
        this.direction = direction;

    }


    /**
     * 检查添加某个极性的边会不会导致循环，如果导致循环，我们需要记录：
     * @param direction 检查的方向
     * @param g DAG图under
     * @return true如果direction被"确定地"否定，其他情况false
     */
    public boolean checkDirection(boolean direction,DependencyGraph g,List<List<DependencyEdge>> derivedPaths) {
        TransactionLT from,to;
        Set<TransactionLT> readFrom;
        if(direction){//writeA->writeB
            from = writeA.parent;
            to = writeB.parent;
            readFrom =  getReadFromA(g);
        }
        else{//writeB->writeA
            from = writeB.parent;
            to = writeA.parent;
            readFrom = getReadFromB(g);
        }
        if(g.checkEdge(from,to,derivedPaths))//try WW
            return true;
        for(TransactionLT read:readFrom){
            if(g.checkEdge(read,to,derivedPaths))//try RW
                return true;
        }
        return false;
    }

    public  boolean checkDirection(boolean direction, DirectedWeightedMultigraph<TransactionLT,DependencyEdge> g){
        return true;
    }

    public boolean checkState() {
        stage++;
        if (stage == 2) {
            this.checkTime = Math.max(writeA.parent.end,writeB.parent.end);
            alive.remove(this);
            toProcess.add(this);
            return true;
        } else
            return false;
    }

    public Set<TransactionLT> getReads(DependencyGraph g, TransactionLT w) {
        Set<TransactionLT> readFrom = new HashSet<>();
        Set<DependencyEdge> outgoingEdges = g.outgoingEdgesOf(w);
        for (DependencyEdge e : outgoingEdges) {
            if (e.isWR())
                readFrom.add(g.getEdgeTarget(e));
        }
        return readFrom;
    }


}
