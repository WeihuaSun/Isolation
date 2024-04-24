package graph.algorithm;

import java.util.*;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;


public class DAGTransitiveClosure {

    int n;
    int[][] index;
    DirectedAcyclicGraph<Integer, DefaultEdge> dag;
    EdgeRecord record;
    Stack<EdgeRecord> stack;

    static class Pair {
        int x;
        int y;

        Pair(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    static class EdgeRecord{
        List<Pair> indexChanges;
        List<Pair> descChanges;

        EdgeRecord(){
            this.indexChanges = new ArrayList<>();
            this.descChanges = new ArrayList<>();
        }
        void record(int x,int y, int z){
            indexChanges.add(new Pair(x,y));
            descChanges.add(new Pair(x,z));
        }

    }

    public DAGTransitiveClosure(DirectedAcyclicGraph<Integer,DefaultEdge> dag ) {
        this.dag = dag;
        this.n = dag.vertexSet().size();
        this.index = new int[n][n];
        this.stack = new Stack<>();
        for (int i = 0; i < n; i++) {
            Arrays.fill(index[i],-1);
        }
    }
    public void pop(int n){
        for(int i=0;i<n;i++){
            EdgeRecord record = stack.pop();
            for(Pair pair:record.indexChanges){
                index[pair.x][pair.y] = -1;
            }

        }
    }

    public boolean isReachable(int s, int t) {
        return s == t || index.get(s).get(t) >= 0;
    }

    public void addTimeEdge(int s,int pos){

    }
    public Set<Integer> getRange(int s,int t){
        return null;
    }

    public void addEdge(int s, int t) {
        record = new EdgeRecord();
        Set<Integer> range = getRange(s,t);
        if (!isReachable(s, t)) {
            //1. isReachable(p, s):p.start<s.end
            //2. !isReachable(p, t):p.end>t.start
            for (int p:range) {
                if (isReachable(p, s) && !isReachable(p, t)) meld(p, t, s, t);
            }
            stack.push(record);
        }
    }

    private void meld(int root, int sub, int u, int v) {
        index.get(root).set(v, u);
        record.record(root,v,u);
        Set<DefaultEdge> outgoingEdges = dag.outgoingEdgesOf(sub);
        for (DefaultEdge edge : outgoingEdges) {
            int c = dag.getEdgeTarget(edge);
            if (!isReachable(root, c)) meld(root, sub, v, c);

        }

    }

}
