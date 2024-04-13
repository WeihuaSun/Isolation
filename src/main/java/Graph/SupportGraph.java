package Graph;


import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class SupportGraph extends DirectedWeightedMultigraph<Integer, DefaultWeightedEdge> {
    public Map<DependencyEdge,DefaultWeightedEdge> edgeMap;
    public Map<DefaultWeightedEdge,DependencyEdge> reEdgeMap;
    public TransactionLT source;
    public TransactionLT target;
    public SupportGraph(Class<? extends DefaultWeightedEdge> edgeClass,TransactionLT source,TransactionLT target) {
        super(edgeClass);
    }

    public HashMap<DependencyEdge,Boolean> getCut(){
        return null;
    }

}
