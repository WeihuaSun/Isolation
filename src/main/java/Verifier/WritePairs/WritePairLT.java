package Verifier.WritePairs;

import Graph.Algorithms.GraphReduce;
import Graph.Edge.DependencyEdge;
import Graph.Node.TransactionLT;
import Graph.Operator.WriteLT;
import Graph.graph.DependencyGraph;
import Graph.graph.SupportGraph;
import Verifier.Exception.ISException;
import Verifier.TxnLevel;

import java.util.*;

public class WritePairLT {
    private final WriteLT writeA;
    private final WriteLT writeB;
    private int stage;
    private boolean direction;
    private Set<TransactionLT> readFromA;
    private Set<TransactionLT> readFromB;
    private boolean isSavePoint;
    private final Set<WritePairLT> alive;//指向主函数
    private final TreeSet<WritePairLT> toProcess;//指向主函数
    private final List<SupportGraph> underSupportTrue = new ArrayList<>();
    private final List<SupportGraph> underSupportFalse= new ArrayList<>();
    private final List<SupportGraph> overSupportTrue= new ArrayList<>();
    private final List<SupportGraph> overSupportFalse= new ArrayList<>();
    private boolean assigned;
    private DependencyGraph g;


    public WritePairLT(WriteLT writeA, WriteLT writeB, TxnLevel verifier){
        this.writeA = writeA;
        this.writeB = writeB;
        this.stage = 0;
        this.readFromA = null;
        this.readFromB = null;
        this.alive = verifier.alivePairs;
        this.toProcess = verifier.toProcessPairs;
        this.isSavePoint = false;
        this.assigned = false;
        this.g = verifier.g;
    }

    private Set<TransactionLT> getReads(DependencyGraph g, TransactionLT w) {
        Set<TransactionLT> readFrom = new HashSet<>();
        Set<DependencyEdge> outgoingEdges = g.outgoingEdgesOf(w);
        for (DependencyEdge e : outgoingEdges) {
            if (e.isWR())
                readFrom.add(g.getEdgeTarget(e));
        }
        return readFrom;
    }
    private Set<TransactionLT> getReadFrom(boolean direction){
        if(direction){
            if(this.readFromA==null){
                this.readFromA =getReads(this.g,this.writeA.parent);
            }
            return this.readFromA;
        }else {
            if(this.readFromB==null){
                this.readFromB =getReads(this.g,this.writeB.parent);
            }
            return this.readFromB;
        }
    }

    /**
     *
     * @param direction true:WriteA->WriteB, false: writeB->writeAl
     * @param g Generalized polygraph
     * @return true if there exists a determinate path support the input direction,this mean that the path is cycled with the edges in another direction
     * otherwise false
     */
    private boolean checkDirection(boolean direction,DependencyGraph g){
        TransactionLT from,to;
        Set<TransactionLT> readFrom = getReadFrom(direction);
        List<SupportGraph> underSupport,overSupport;
        if(!direction){//writeA->writeB
            from = writeA.parent;
            to = writeB.parent;
            underSupport = this.underSupportTrue;
            overSupport = this.overSupportTrue;
        }else {
            from = writeB.parent;
            to = writeA.parent;
            underSupport = this.underSupportFalse;
            overSupport = this.overSupportFalse;
        }
        //ww
        GraphReduce gr = new GraphReduce(g,to,from);
        //SupportGraph over = gr.getSupportGraph("over");
        //SupportGraph under = gr.getSupportGraph("under");
        if(gr.getReachability()){
            SupportGraph over = gr.getSupportGraph("over");
            if(over==null)
                return true;
            else{
                SupportGraph under = gr.getSupportGraph("under");
                overSupport.add(over);
                underSupport.add(under);
            }
        }

        for(TransactionLT read:readFrom){
            gr = new GraphReduce(g,to,read);
            if(gr.getReachability()){
                SupportGraph over = gr.getSupportGraph("over");
                if(over==null)
                    return true;
                else{
                    SupportGraph under = gr.getSupportGraph("under");
                    overSupport.add(over);
                    underSupport.add(under);
                }
            }
        }
        return false;
    }

    private void backTrace(boolean determinate){
        backTrace(this.direction,determinate);
    }
    private void backTrace(boolean direction,boolean determinate){
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
            reverseDirection(determinate);

    }

    private void removeDirection(boolean determinate,boolean direction){
        TransactionLT from,to;
        Set<TransactionLT> readFrom = getReadFrom(direction);
        if(direction){
            from = writeA.parent;
            to=writeB.parent;
        }else{
            from = writeB.parent;
            to = writeA.parent;
        }
        if(determinate){
            g.removeEdge(from,to);
            for(TransactionLT read:readFrom){
                g.removeEdge(read,to);
            }
        }else{
            g.setEdge(from,to, DependencyEdge.State.Undetermined);
            for(TransactionLT read:readFrom){
                g.setEdge(read,to, DependencyEdge.State.Undetermined);
            }
        }
    }
    private void setDirection(boolean determinate,boolean direction){
        TransactionLT from,to;
        Set<TransactionLT> readFrom = getReadFrom(direction);
        if(direction){
            from = writeA.parent;
            to=writeB.parent;
        }else{
            from = writeB.parent;
            to = writeA.parent;
        }
        if(determinate){
            g.setEdge(from,to, DependencyEdge.State.Determinate);
            for(TransactionLT read:readFrom){
                g.setEdge(read,to, DependencyEdge.State.Determinate);
            }
        }else{
            g.setEdge(from,to, DependencyEdge.State.Derived);
            for(TransactionLT read:readFrom){
                g.setEdge(read,to, DependencyEdge.State.Derived);
            }
        }
    }

    private void reverseDirection(boolean determinate){
        this.removeDirection(determinate,this.direction);
        this.setDirection(determinate,this.direction);
        DependencyEdge e;
        if(direction){
            //delete old edge
            if(determinate){
                g.removeEdge(writeA.parent,writeB.parent);

            }
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

    public void setStage(int stage) {
        this.stage = stage;
    }
}
