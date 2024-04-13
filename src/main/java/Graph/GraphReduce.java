package Graph;

import org.jgrapht.alg.flow.EdmondsKarpMFImpl;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.*;

public class GraphReduce {
    public DependencyGraph g;//依赖图G
    private int vertexId;//全局连通分量编号
    private Set<Long> visited;//访问过的节点
    private Set<Long> cVisited;//可以到达目标的已经访问过的节点
    private Set<Long> nVisited;//不可以到达目标的已经访问过的节点
    private Map<Long, VertexId> vertexMap;
    private Map<VertexId, Set<VertexId>> reVertexMap;

    private final long left;
    private final long right;
    private final TransactionLT source;
    private final TransactionLT target;
    private String stage = "over";

    private Set<DependencyEdge> supportEdges;
    private final Reachability reachability;

    public GraphReduce(DependencyGraph g, TransactionLT source, TransactionLT target) {
        this.g = g;
        this.source = source;
        this.target = target;
        this.left = Math.min(source.start, target.start);
        this.right = Math.max(source.end, target.end);
        this.reachability = dfs(source, null);
    }

    private void reset(String stage) {
        visited = new HashSet<>();
        cVisited = new HashSet<>();
        nVisited = new HashSet<>();
        cVisited.add(target.txnId);
        supportEdges = new HashSet<>();
        this.vertexMap = new HashMap<>();
        this.reVertexMap = new HashMap<>();
        this.vertexId = 0;
        this.stage = stage;
    }

    public SupportGraph getSupportGraph(String type){
        assert type.equals("over")||type.equals("under");
        reset(type);
        return genGraph();
    }


    private SupportGraph genGraph() {
        if (reVertexMap.size() == 1) {
            return null;
        } else {
            SupportGraph sGraph = new SupportGraph(DefaultWeightedEdge.class,source,target);
            for (Map.Entry<VertexId, Set<VertexId>> vertexIdSetEntry : reVertexMap.entrySet()) {
                sGraph.addVertex(vertexIdSetEntry.getKey().id);
            }
            for (DependencyEdge edge : supportEdges) {
                long head = g.getEdgeTarget(edge).txnId;
                long end = g.getEdgeSource(edge).txnId;
                if (vertexMap.get(head) != vertexMap.get(end)) {
                    DefaultWeightedEdge e = sGraph.addEdge(vertexMap.get(end).id, vertexMap.get(head).id);
                    sGraph.edgeMap.put(edge,e);
                    sGraph.reEdgeMap.put(e,edge);
                }
            }
            return sGraph;
        }
    }

    public boolean getReachability() {
        return reachability==Reachability.Reachable;
    }

    static class VertexId implements Graph.VertexId {
        int id;

        public VertexId(int id) {
            this.id = id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object object) {
            assert object instanceof VertexId;
            return this.id == ((VertexId) object).id;
        }
    }

    public enum cState {
        uDReach, Reach, UnReach
    }

    public enum Reachability {
        Reachable, Unreachable, Unknown
    }

    public cState checkState(DependencyEdge edge) {
        if (stage.equals("over")) {
            if (edge.getState() == DependencyEdge.State.Undetermined || edge.getState() == DependencyEdge.State.Derived)
                return cState.Reach;
            else return cState.uDReach;
        }
        if (stage.equals("under")) if (edge.getState() == DependencyEdge.State.Derived) return cState.Reach;
        else if (edge.getState() == DependencyEdge.State.Determinate) return cState.uDReach;
        else return cState.UnReach;
        return cState.UnReach;
    }

    private void merge(VertexId source, VertexId target) {
        for (VertexId vId : reVertexMap.get(target)) {
            vId.setId(source.id);
        }
        Set<VertexId> toMerge = reVertexMap.get(source);
        toMerge.addAll(reVertexMap.remove(target));
        reVertexMap.put(source, toMerge);
    }

    private Reachability dfs(TransactionLT source, VertexId sourceId) {
        boolean reach = false;
        boolean unknown = false;
        boolean uDReach = false;
        if (sourceId == null) {
            uDReach = true;
            sourceId = new VertexId(++vertexId);
        }
        if (source.end < left || source.start > right) {
            nVisited.add(source.txnId);
            return Reachability.Unreachable;
        }
        if (nVisited.contains(source.txnId)) return Reachability.Unreachable;
        if (cVisited.contains(source.txnId)) {
            if (uDReach) {//入边为不确定的边，sourceId被改变
                if (vertexMap.containsKey(source.txnId)) {//已经被分配
                    //DoNothing
                } else {//没被分配
                    vertexMap.put(source.txnId, sourceId);//分配
                    if (!reVertexMap.containsKey(sourceId)) {
                        HashSet<VertexId> newSet = new HashSet<>();
                        newSet.add(sourceId);
                        reVertexMap.put(sourceId, newSet);
                    }
                }
            } else {//入边为确定的边
                if (vertexMap.containsKey(source.txnId)) {//已经被分配
                    if (!vertexMap.get(source.txnId).equals(sourceId)) {
                        merge(sourceId, vertexMap.get(source.txnId));
                    }
                } else {//没被分配
                    vertexMap.put(source.txnId, sourceId);//分配
                    if (!reVertexMap.containsKey(sourceId)) {
                        HashSet<VertexId> newSet = new HashSet<>();
                        newSet.add(sourceId);
                        reVertexMap.put(sourceId, newSet);
                    }
                }
            }
            return Reachability.Reachable;
        }
        if (visited.contains(source.txnId)) return Reachability.Unknown;
        visited.add(source.txnId);
        for (DependencyEdge outEdge : g.outgoingEdgesOf(source)) {
            TransactionLT head = g.getEdgeTarget(outEdge);
            cState state = checkState(outEdge);
            Reachability result = Reachability.Unreachable;
            switch (state) {
                case uDReach://通过确定的边连接
                    result = dfs(head, sourceId);
                    break;
                case Reach:
                    result = dfs(head, null);
                    if (result == Reachability.Reachable) {
                        supportEdges.add(outEdge);
                    }
                    break;
                //new VertexId(++vertexId)
                case UnReach:
                    break;
            }
            if (result == Reachability.Reachable) {
                cVisited.add(source.txnId);
                if(!reach){
                    if (uDReach) {//入边为不确定的边，sourceId被改变
                        if (vertexMap.containsKey(source.txnId)) {//已经被分配
                            //DoNothing
                        } else {//没被分配
                            vertexMap.put(source.txnId, sourceId);//分配
                            if (!reVertexMap.containsKey(sourceId)) {
                                HashSet<VertexId> newSet = new HashSet<>();
                                newSet.add(sourceId);
                                reVertexMap.put(sourceId, newSet);
                            }
                        }
                    } else {//入边为确定的边
                        if (vertexMap.containsKey(source.txnId)) {//已经被分配
                            if (!vertexMap.get(source.txnId).equals(sourceId)) {
                                merge(sourceId, vertexMap.get(source.txnId));
                            }
                        } else {//没被分配
                            vertexMap.put(source.txnId, sourceId);//分配
                            if (!reVertexMap.containsKey(sourceId)) {
                                HashSet<VertexId> newSet = new HashSet<>();
                                newSet.add(sourceId);
                                reVertexMap.put(sourceId, newSet);
                            }
                        }

                    }
                    reach = true;
                }

            }
            else if (result == Reachability.Unreachable) {
                continue;
            } else {//Unknown
                unknown = true;
            }
        }
        visited.remove(source.txnId);

        if (reach) {
            return Reachability.Reachable;
        } else {
            if (unknown) return Reachability.Unknown;
            nVisited.add(source.txnId);
            return Reachability.Unreachable;
        }

    }

}
