package Graph;

import org.jgrapht.GraphPath;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class MultiDependencyGraph extends DirectedWeightedMultigraph<TransactionLT,DependencyEdge> {
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
    public Set<TransactionLT> dfs(TransactionLT s,TransactionLT t,DependencyGraph cg,Set<Long> visited,Set<Long> cVisited){
        Set<TransactionLT> reach = new HashSet<>();
        if(s.equals(t))
            return reach; //emptyList
        if(visited.contains(s.txnId))
            return null;
        else
            visited.add(s.txnId);

        for(DependencyEdge outEdge:this.outgoingEdgesOf(s)){
            TransactionLT edgeEnd = this.getEdgeTarget(outEdge);
            Set<TransactionLT> cNodes = dfs(edgeEnd,t,cg,visited,cVisited);
            if(!outEdge.isDeterminate()){
                cg.addEdge(s,edgeEnd);
                reach.add(edgeEnd);
            }
            else{
                if(cNodes!=null)
                    reach.addAll(cNodes);
            }

        }
        cVisited.add(s.txnId);
        visited.remove(s.txnId);
        return reach;
    }


}
