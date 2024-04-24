package Verifier.writePair;

import graph.edge.DependencyEdge;
import graph.vertex.TransactionLT;
import org.jgrapht.graph.DefaultEdge;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class writePairOffline {
    public Map<DirectionType, Direction> directions;
    public enum DirectionType {a,b,unknown};
    public DirectionType direction;

    public writePairOffline(){
        this.directions = new HashMap<>();
        this.direction = DirectionType.unknown;
    }
    public void addEdge(DependencyEdge e, TransactionLT head, TransactionLT tail, DirectionType type){
        assert type!=DirectionType.unknown;
        this.directions.get(type).addEdge(e,head,tail);
    }
    public Set<Pair<TransactionLT,TransactionLT>> getSupport(DirectionType type){
        assert type!=DirectionType.unknown;
        if (type == DirectionType.a)
            return this.directions.get(DirectionType.b).opposeReach;
        else
            return this.directions.get(DirectionType.a).opposeReach;
    }

    public static class Direction<E extends DefaultEdge>{
        public Set<E> edgeSet;
        public int id;
        public int level;
        public Set<Direction<E>> reason;
        public writePairOffline parent;
        public Set<Pair<TransactionLT,TransactionLT>> opposeReach;
        public Direction(){
            this.edgeSet = new HashSet<>();
            this.opposeReach = new HashSet<>();
        }
        public void addEdge(E e,TransactionLT head,TransactionLT tail){
            this.edgeSet.add(e);
            this.opposeReach.add(new Pair<>(head, tail));
        }
        public Set<Direction<E>> getCoReason(){
            return null;
        }
    }

    public record Pair<X, Y>(X head, Y tail) {}

}
