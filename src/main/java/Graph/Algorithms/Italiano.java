package Graph.Algorithms;

import Graph.Edge.Edge;
import Graph.Node.Vertex;
import Graph.graph.BaseGraph;


public class Italiano<V extends Vertex,E extends Edge> {
    public  BaseGraph<V,E> g;
    private boolean reset;
    private E[][] parent;
    private Boolean[][] desc;
    public Italiano(BaseGraph<V,E> g){
        this.g = g;

    }
    private void insertEdge(E e){
        reset = true;
        V s = g.getEdgeSource(e);
        V t = g.getEdgeTarget(e);
        if(!desc[s.id][t.id])
            for(V v : g.vertexSet()){
                if (desc[v.id][s.id] && (!desc[v.id][t.id])) meld(v,e);
            }
    }
    private void meld(V v,E e){
        V t = this.g.getEdgeTarget(e);
        desc[v.id][t.id] = true;
        parent[v.id][t.id] = e;
        for(E oe:g.outgoingEdgesOf(t)){
            if(!desc[v.id][g.getEdgeTarget(oe).id])
                meld(v,oe);
        }
    }
}
