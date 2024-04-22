package Graph.Algorithms;

import Graph.Node.Vertex;
import Graph.Edge.Edge;
import Graph.graph.BaseGraph;
import Graph.Index;
import org.jgrapht.graph.AbstractBaseGraph;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

@SuppressWarnings("unchecked")
public class ItalianoStack<V extends Vertex,E extends Edge> {
    public BaseGraph<V,E> g;
    private final E[][] parent;
    private final boolean[][] desc;
    private InsertRecord record;
    private final Stack<InsertRecord> stack;
    public ItalianoStack(BaseGraph<V,E> g, Class<? extends E> edgeClass){
        this.g = g;
        int size = g.vertexSet().size();
        this.parent = (E[][]) Array.newInstance(edgeClass,size,size);
        this.desc = new boolean[size][size];
        for (boolean[] row : desc) {
            Arrays.fill(row, false);
        }
        this.stack = new Stack<>();
    }
    public List<Index> insertEdge(E e){
        record = new InsertRecord();
        V s = g.getEdgeSource(e);
        V t = g.getEdgeTarget(e);
        if(!desc[s.id][t.id])
            for(V v : g.vertexSet()){
                if (desc[v.id][s.id] && (!desc[v.id][t.id])) meld(v,e);
            }
        return record.getRecord();
    }
    private void meld(V v,E e){
        V t = this.g.getEdgeTarget(e);
        record.add(v.id,t.id);
        desc[v.id][t.id] = true;
        parent[v.id][t.id] = e;
        for(E oe:g.outgoingEdgesOf(t)){
            if(!desc[v.id][g.getEdgeTarget(oe).id])
                meld(v,oe);
        }
    }
    public void backtrace(int n){
        for(int i=0;i<n;i++){
            InsertRecord r = stack.pop();
            for(Index index :r.getRecord()){
                parent[index.x()][index.y()] = null;
                desc[index.x()][index.y()] = false;
            }
        }
    }
    static class InsertRecord{
        List<Index> record;
        InsertRecord(){
            this.record = new ArrayList<>();
        }
        void add(int x,int y){
           this.record.add(new Index(x,y));
        }
        List<Index> getRecord(){
            return this.record;
        }
    }
}

