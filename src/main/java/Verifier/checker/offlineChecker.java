package Verifier.checker;

import Verifier.gMinSAT.GMinSAT;
import Verifier.writePair.writePairOffline.Direction;
import graph.Index;
import graph.algorithm.ItalianoStack;
import graph.vertex.Vertex;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultDirectedGraph;


import java.util.*;

import org.logicng.collections.*;
import org.logicng.formulas.FormulaFactory;
import org.logicng.solvers.MiniSat.*;
import org.logicng.solvers.MiniSat;
import org.logicng.solvers.SATSolver;

public class offlineChecker<V extends Vertex,E extends DefaultEdge> {
    private final DefaultDirectedGraph<V,E> under;
    private final DefaultDirectedGraph<V,E> over;
    private final ItalianoStack<V,E> transitiveClosure;
    private final List<V> vertices;
    private final LNGIntVector gTrail;
    private final LNGIntVector gTrailLim;
    private final SATSolver solver;
    private final GMinSAT gMinSAT = new GMinSAT();
    protected int qhead;
    private Queue<E> knownEdges;
    private Queue<E> edgeQueue;

    private Map<Index,Direction<E>> directionMap;
    private LNGBooleanVector seen;

    public offlineChecker(Class<? extends E> edgeClass){
        this.under = new DefaultDirectedGraph<>(edgeClass);
        this.over = new DefaultDirectedGraph<>(edgeClass);
        this.transitiveClosure = new ItalianoStack<>(under,edgeClass);
        this.vertices = new ArrayList<>();
        this.gTrail = new LNGIntVector();
        this.gTrailLim = new LNGIntVector();
        this.edgeQueue = new LinkedList<>();
        this.knownEdges = new LinkedList<>();
        FormulaFactory f = new FormulaFactory();
        this.solver =  MiniSat.miniSat(f);
        this.seen = new LNGBooleanVector();
    }

    protected int decisionLevel() {
        return this.gTrailLim.size();
    }

    private boolean simplify(){
        while (!knownEdges.isEmpty()){
            E edge = knownEdges.poll();
            V s = over.getEdgeSource(edge);
            V t = over.getEdgeTarget(edge);
            if (transitiveClosure.reachable(t,s)){//形成环路
                return false;
            }else{
                List<Index> changes =  transitiveClosure.insertEdge(edge);
                for(Index cIndex:changes){
                    Direction<E> direction = directionMap.remove(cIndex);
                    if(direction!=null){
                        knownEdges.addAll(direction.edgeSet);
                    }
                }
            }
        }
        return true;
    }


    private boolean propagate(){
        while (!edgeQueue.isEmpty()){
            E edge = edgeQueue.poll();
            V s = over.getEdgeSource(edge);
            V t = over.getEdgeTarget(edge);
            if (transitiveClosure.reachable(t,s)){//形成环路
                return false;
            }else{
                List<Index> changes =  transitiveClosure.insertEdge(edge);
                for(Index cIndex:changes){
                    Direction<E> direction = directionMap.remove(cIndex);
                    if(direction!=null){
                        edgeQueue.addAll(direction.edgeSet);
                    }
                }
            }
        }
        return true;

    }
    private void analyze(Direction<E> conflict,final LNGIntVector outLearnt ){
        int pathC = 0;
        outLearnt.push(-1);
        int index = this.gTrail.size() - 1;
        Set<Direction<E>> iReason = conflict.getCoReason();
        do {
            for(Direction<E> q:iReason){
                if(!this.seen.get(q.id) && q.level>0){
                    this.seen.set(q.id,true);
                    if(q.level>decisionLevel()){
                        pathC++;
                    }else{
                        outLearnt.push(q.id);
                    }
            }
            }
            while (!this.seen.get(this.gTrail.get(index--))) {}
            Direction<E> p= null;
            iReason = p.reason;
            this.seen.set(p.id, false);
            pathC--;
        } while (pathC > 0);
    }


    protected void solve(){

    }




}
