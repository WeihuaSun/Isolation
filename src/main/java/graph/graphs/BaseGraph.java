package graph.graphs;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.function.Supplier;

public class BaseGraph<V,E> extends DefaultDirectedGraph<V,E> {

    public BaseGraph(Class<? extends E> edgeClass) {
        super(edgeClass);
    }

    public BaseGraph(Supplier<V> vertexSupplier, Supplier<E> edgeSupplier, boolean weighted) {
        super(vertexSupplier, edgeSupplier, weighted);
    }
}
