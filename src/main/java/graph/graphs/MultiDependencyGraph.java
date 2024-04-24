package graph.graphs;

import graph.vertex.TransactionLT;
import graph.edge.DependencyEdge;
import org.jgrapht.GraphPath;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class MultiDependencyGraph extends DirectedWeightedMultigraph<TransactionLT, DependencyEdge> {
    public MultiDependencyGraph(Class<? extends DependencyEdge> edgeClass) {
        super(edgeClass);
    }
    public MultiDependencyGraph(Supplier<TransactionLT> vertexSupplier, Supplier<DependencyEdge> edgeSupplier) {
        super(vertexSupplier, edgeSupplier);
    }
    public MultiDependencyGraph(){
        super(DependencyEdge.class);
    }
    public List<GraphPath<TransactionLT,DependencyEdge>> getAllPaths(TransactionLT s,TransactionLT t){
        AllDirectedPaths<TransactionLT,DependencyEdge> allDirectedPaths = new AllDirectedPaths<>(this);
        return allDirectedPaths.getAllPaths(s,t,true,null);
    }
    public boolean checkPath(TransactionLT s,TransactionLT t,MultiDependencyGraph over,MultiDependencyGraph under){
        DependencyGraph cg = new DependencyGraph(DependencyEdge.class);
        Set<Long> visited = new HashSet<>();
        Set<Long> cVisited = new HashSet<>();
        Set<TransactionLT> reach = new HashSet<>();
        cg.addVertex(s);
        cg.addVertex(t);
        boolean isD = true;
        for(DependencyEdge outEdge:this.outgoingEdgesOf(s)){
            TransactionLT edgeEnd = this.getEdgeTarget(outEdge);
            Set<TransactionLT> cNodes = dfs(edgeEnd,t,cg,visited,cVisited);
            if(!outEdge.isDeterminate()){
                if(cNodes!=null){
                    isD = false;
                    cg.addEdge(s,edgeEnd);
                    reach.add(edgeEnd);
                }
            }
            else{
                if(cNodes!=null)
                    reach.addAll(cNodes);
            }
        }
        dfs(s,t,cg,visited,cVisited);
        return isD;
    }

    public List<Set<TransactionLT>> dfs(TransactionLT s, TransactionLT t, SupportGraph over, SupportGraph under, Set<Long> visited, Set<Long> cVisited, long left, long right){
        List<Set<TransactionLT>> reaches= new ArrayList<>();
        Set<TransactionLT> reachOver = new HashSet<>();
        Set<TransactionLT> reachUnder = new HashSet<>();
        reaches.add(reachOver);
        reaches.add(reachUnder);
        if(s.equals(t))
            return reaches; //emptyList,but not null
        if(visited.contains(s.txnId)||s.end<=left||s.start>=right)
            return null;//null,mean this vertex cannot reach the target
        else
            visited.add(s.txnId);

        for(DependencyEdge outEdge:this.outgoingEdgesOf(s)){
            TransactionLT edgeEnd = this.getEdgeTarget(outEdge);
            if(t.equals(edgeEnd)){
                if(outEdge.getState()== DependencyEdge.State.Undetermined){
                    Long [] st = new Long[2];
                    st[0] = s.txnId;
                    st[1] = edgeEnd.txnId;
                    over.edgeDict.put(st ,outEdge);
                }
            }
            List<Set<TransactionLT>> cNodes= dfs(edgeEnd,t,over,under,visited,cVisited,left,right);
            if(outEdge.getState()== DependencyEdge.State.Undetermined){
                over.addEdge(s,edgeEnd);
                reachOver.add(edgeEnd);
            } else if (outEdge.getState() == DependencyEdge.State.Derived) {
                over.addEdge(s,edgeEnd);
                reachOver.add(edgeEnd);
                under.addEdge(s,edgeEnd);
                reachUnder.add(edgeEnd);
            } else{
                if(cNodes!=null){
                    reachOver.addAll(cNodes.getFirst());
                    reachUnder.addAll(cNodes.getLast());
                }
            }
        }
        cVisited.add(s.txnId);
        visited.remove(s.txnId);
        return reaches;
    }


}
