package Graph;

import Verifier.ISException;
import com.sun.source.tree.Tree;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import java.util.*;

//

public class WritePair {
    ArrayList<WritePair> parent;
    boolean isSavePoint =false;
    WriteLT writeA;
    WriteLT writeB;
    long endTime;
    int stage = 0;
    public boolean direction;
    long checkTime;
    Set<TransactionLT> readFromA;
    Set<TransactionLT> readFromB;

    private boolean assigned = false;

    private List<SupportGraph> underSupportTrue;
    private List<SupportGraph> underSupportFalse;
    private List<SupportGraph> overSupportTrue;
    private List<SupportGraph> overSupportFalse;


    public WritePair(WriteLT writeA, WriteLT writeB) {
        this.writeA = writeA;
        this.writeB = writeB;
        this.endTime = Math.max(writeA.parent.end, writeB.parent.end);
        this.readFromA = null;
        this.readFromB = null;
    }
    private boolean checkDirection(boolean direction,DependencyGraph g){
        return true;
    }
    private void reverseDirection(boolean determinate,DependencyGraph g){

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
    private void backTrace(){
        backTrace(this.direction);
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





//    private boolean checkSupportOver(boolean direction,DependencyEdge e){
//        boolean flag = false;
//        Set<Set<DependencyEdge>> udPaths;
//        if(direction){
//            udPaths = overSupportA;
//        }
//        else{
//            udPaths = overSupportB;
//        }
//
//        return flag;
//    }

    public boolean checkEdge(DependencyEdge e,List<Set<DependencyEdge>> paths){
        boolean flag = false;
        for (Set<DependencyEdge> edgeSet : paths) {
            edgeSet.remove(e);
            if (edgeSet.isEmpty()) {
                flag = true;
            }
        }
        return flag;
    }

    public boolean checkEdge(DependencyEdge e){
        return true;
    }




    public void clear(){
        //TODO:删除该writePair
    }

    //设置检查点
    public void setSavePoint(){
        this.isSavePoint = true;
    }
    public void clearSavePoint(){
        this.isSavePoint = false;
    }


    public boolean backtrace(){
        return true;
    }

    /**
     * Step1.删除掉旧的方向相关的Edge
     * Step2.添加相反方向的相关的Edge
     * @param determinate 是否确定的反向
     * @param under under
     * @param over over
     */
    private void reverseDirection(boolean determinate, DependencyGraph under, MultiDependencyGraph over){
        DependencyEdge e;
        if(direction){
            //delete old edge
            under.removeEdge(writeA.parent,writeB.parent);
            if(determinate)
                over.removeEdge(writeA.parent,writeB.parent);
            for(TransactionLT t:readFromA){
                under.removeEdge(t,writeB.parent);
                if(determinate)
                    over.removeEdge(t,writeB.parent);
            }
            //add new edge
            e = over.getEdge(writeB.parent,writeA.parent);
            e.setDeterminate(determinate);
            under.addEdge(writeB.parent,writeA.parent,e);
            for(TransactionLT t:readFromB){
                e = over.getEdge(t,writeA.parent);
                e.setDeterminate(determinate);
                under.addEdge(t,writeA.parent,e);
            }
            this.direction = false;
        }
        else{
            //delete old edge
            under.removeEdge(writeB.parent,writeA.parent);
            if(determinate)
                over.removeEdge(writeB.parent,writeA.parent);
            for(TransactionLT t:readFromB){
                under.removeEdge(t,writeA.parent);
                if(determinate)
                    over.removeEdge(t,writeA.parent);
            }
            //add new edge
            e = over.getEdge(writeA.parent,writeB.parent);
            e.setDeterminate(determinate);
            under.addEdge(writeA.parent,writeB.parent,e);
            for(TransactionLT t:readFromA){
                e = over.getEdge(t,writeB.parent);
                e.setDeterminate(determinate);
                under.addEdge(t,writeB.parent,e);
            }
            this.direction = true;
        }
    }

    /**
     * 回溯：如果在Under中出现了环，则需要根据检查点进行回溯
     * @param determinate 是否是确定性的回溯，如果某个方向被确定性的否定，那么需要清除该检查点，并回溯
     * @param under under图，存储确定的和派生的边,DAG
     * @param over over图，存储所有可能的边，包含under中的所有边
     */
    public void backTrace(boolean determinate,DependencyGraph under,MultiDependencyGraph over) {
        if(determinate)
            clearSavePoint();//如果是确定的回溯，那么删除该检查点
        Set<DependencyEdge> toBack = new HashSet<>();//存储已经回溯的边
        Iterator<Set<DependencyEdge>> iterator;
        if (direction) {
            iterator = underSupportA.iterator();
        } else {
            iterator = underSupportB.iterator();
        }
        while (iterator.hasNext()){
            Set<DependencyEdge> path = iterator.next();
            Iterator<DependencyEdge> subIterator = path.iterator();
            if (path.size() == 1) {//如果该路径只有一条边，那么该边必须作为割集的一部分，所以确定的回溯该边对应的writePair
                DependencyEdge e = subIterator.next();
                toBack.add(e);
                for (WritePair wp : e.writePairs) {
                    wp.backTrace(true,under,over);
                }
            }
            else {
                if(Collections.disjoint(path,toBack)){//如果path和已经删除的边没有交集，那么需要选择一条边，加入割集，因为不只有一条边，所以是非确定的回溯，但必须有一个回溯
                    DependencyEdge e = subIterator.next();
                    toBack.add(e);
                    for (WritePair wp : e.writePairs) {
                        wp.backTrace(false,under,over);
                    }
                }
            }
        }
        if(assigned)
            reverseDirection(determinate,under,over);
    }

    public void backTrace(boolean determinate,boolean direction,DependencyGraph under,MultiDependencyGraph over){
        this.direction = direction;
        backTrace(determinate,under,over);
    }

//    public boolean getCut(Set<DependencyEdge> edges){
//        if(direction){
//            Set<DependencyEdge> path = underSupportA.first();
//            if(path.size() == 1){
//               DependencyEdge e = path.iterator().next();
//               for(WritePair wp:e.writePairs){
//                   if(!wp.dBacktrace()){
//                       return false;
//                   }
//               }
//
//            }
//        }
//    }
//    public boolean dBacktrace(){
//        Set<DependencyEdge> cut = new HashSet<>();
//        int cost = 0;
//        if(cost>DependencyEdge.Large){
//            return false;
//        }else {
//            Set<WritePair> toBacktrace = new HashSet<>();
//            for(DependencyEdge e:cut){
//                toBacktrace.addAll(e.writePairs);
//            }
//            for (WritePair wp:toBacktrace){
//                wp.backtrace();
//            }
//        }
//    }
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
    public void addEdge(DependencyGraph g,TransactionLT sourceVertex, TransactionLT targetVertex,DependencyEdge.Type type){

            DependencyEdge edge = g.getEdge(sourceVertex,targetVertex);
            if(edge!=null){
                edge.setType(type);
            }
            else{
                g.addEdge(sourceVertex,targetVertex,new DependencyEdge(type));
            }

    }
    public void setDirection(boolean direction,DirectedMultigraph<TransactionLT,DependencyEdge> g){
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

    public  boolean checkDirection(boolean direction, DirectedMultigraph<TransactionLT,DependencyEdge> g){
        return true;
    }

    public boolean checkState() {
        stage++;
        if (stage == 2) {
            parent.remove(this);
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
