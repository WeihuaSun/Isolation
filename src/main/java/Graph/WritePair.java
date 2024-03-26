package Graph;

import org.jgrapht.graph.DirectedMultigraph;

import java.util.*;

public class WritePair {

    ArrayList<WritePair> parent;
    WriteLT writeA;
    WriteLT writeB;
    long endTime;
    int stage = 0;
    public boolean direction;
    long checkTime;
    Set<TransactionLT> readFromA;
    Set<TransactionLT> readFromB;

    public WritePair(WriteLT writeA, WriteLT writeB) {
        this.writeA = writeA;
        this.writeB = writeB;
        this.endTime = Math.max(writeA.parent.end, writeB.parent.end);
        this.readFromA = null;
        this.readFromB = null;
    }

    public void clear(){
        //TODO:删除该writePair
    }
    public void setSavePoint(){

    }
    public void backtrace(){

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
