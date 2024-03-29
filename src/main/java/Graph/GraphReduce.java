package Graph;

import java.util.*;

public class GraphReduce {
    public DependencyGraph g;
    public int overVertexId;
    public int underVertexId;
    public Set<Long> overVisited;//访问过的节点
    public Set<Long> cOverVisited;//可以到达目标的已经访问过的节点
    public Set<Long> nOverVisited;//不可以到达目标的已经访问过的节点

    public Set<Long> underVisited;//访问过的节点
    public Set<Long> cUnderVisited;//可以到达目标的已经访问过的节点
    public Set<Long> nUnderVisited;//不可以到达目标的已经访问过的节点
    public Set<Long> assigned;

    public SupportGraph over;
    public SupportGraph under;
    private final Map<Long,VertexId> overVertexMap;
    private final Map<Long,VertexId> underVertexMap;


    private Map<VertexId,Set<VertexId>> reOverVertexMap;
    private Map<VertexId,Set<VertexId>> reUnderVertexMap;
    public long left;
    public long right;
    public TransactionLT source;
    public TransactionLT target;
    public Set<DependencyEdge> overEdges;
    public Set<DependencyEdge> underEdges;

    static class VertexId{
        int id;
        public VertexId(int id){
            this.id = id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }

    public GraphReduce(DependencyGraph g,long left,long right,TransactionLT source,TransactionLT target){
        this.g = g;
        this.visited = new HashSet<>();
        this.cVisited = new HashSet<>();
        this.overVertexMap = new HashMap<>();
        this.underVertexMap = new HashMap<>();
        this.assigned = new HashSet<>();
        this.left = left;
        this.right = right;
        this.source =source;
        this.target = target;
        this.overVertexId = 0;
        this.underVertexId = 0;
    }




    private int dfs(TransactionLT source,VertexId overSourceId){
        int overReach = -1;
        if (source.end < left || source.start > right)
            return -1;
        if(overVisited.contains(source.txnId))
            return 0;
        boolean unknown = false;
        overVisited.add(source.txnId);
        for (DependencyEdge outEdge : g.outgoingEdgesOf(source)) {
            TransactionLT head = g.getEdgeTarget(outEdge);
            if (outEdge.getState() == DependencyEdge.State.Undetermined||outEdge.getState()== DependencyEdge.State.Derived){
                if(cOverVisited.contains(head.txnId)){
                    overReach =1;
                    VertexId overHeadId = overVertexMap.get(head.txnId);
                    if (overHeadId.id != overSourceId.id) {//二者连通但不属于同一个集合,合并Source到Head(可能选一个小的集合更快)
                        for (VertexId l : reOverVertexMap.get(overHeadId)) {
                            l.setId(overSourceId.id);//更改source的Id为Head
                        }
                        //合并集合
                        Set<VertexId> mergeVertex = reOverVertexMap.get(overSourceId);
                        mergeVertex.addAll(reOverVertexMap.remove(overHeadId));
                        reOverVertexMap.put(overSourceId,mergeVertex);
                    }
                else if(nOverVisited.contains(head.txnId)){
                    continue;
                    }
                }else{
                    int reachable = dfs(head,overSourceId);
                    if(reachable==1){
                        overReach = 1;
                    }else if(reachable == 0){
                        unknown = true;
                    }else if(reachable==-1){
                        nOverVisited.add(head.txnId);
                    }
                }
            }else {//determinate
                if(cOverVisited.contains(head.txnId)){
                    continue;
                }else{
                    int reachable = dfs(head,new VertexId(++overVertexId));
                    if(reachable==1){
                        overReach = 1;
                    }else if(reachable == 0){
                        unknown = true;
                    }else {
                        nOverVisited.add(head.txnId);
                    }
                }
            }

        }

        if(!unknown){
            if(overReach==1){
                cOverVisited.add(source.txnId);
                overVisited.remove(source.txnId);
            }else {
                nOverVisited.add(source.txnId);
            }
        }
        return unknown? 0:overReach;
    }




    /**
     *
     * @param source
     * @param overSourceId
     * @param underSourceId
     * @return
     */
    private ReachInfo dfs(TransactionLT source,VertexId overSourceId,VertexId underSourceId) {
        int overReach = -1;
        int underReach = -1;
        if (source.end < left || source.start > right)
            return new ReachInfo(-1, -1);
        if (overVisited.contains(source.txnId)) overReach = 0;
        if (underVisited.contains(source.txnId)) underReach = 0;
        overVisited.add(source.txnId);
        underVisited.add(source.txnId);
        for (DependencyEdge outEdge : g.outgoingEdgesOf(source)){
            TransactionLT head = g.getEdgeTarget(outEdge);
            if (outEdge.getState() == DependencyEdge.State.Undetermined){
                if(cVisited.contains(head.txnId)){
                    VertexId overHeadId = overVertexMap.get(head.txnId);
                    if (overHeadId.id != overSourceId.id) {//二者连通但不属于同一个集合,合并Source到Head(可能选一个小的集合更快)
                        for (VertexId l : reOverVertexMap.get(overHeadId)) {
                            l.setId(overSourceId.id);//更改source的Id为Head
                        }
                        //合并集合
                        Set<VertexId> mergeVertex = reOverVertexMap.get(overSourceId);
                        mergeVertex.addAll(reOverVertexMap.remove(overHeadId));
                        reOverVertexMap.put(overSourceId,mergeVertex);
                    }
                }
                else if(dfs(head,overSourceId,underSourceId)==1){//该节点可以到达Target
                    reach = 1;
                    //此时head的Id必然等于source的Id
//                    Set<VertexId> mergeVertex = reOverVertexMap.get(overSourceId);
//                    VertexId overHeadId = overVertexMap.get(head.txnId);
//                    mergeVertex.add(overHeadId);
                }
            } else if (outEdge.getState() == DependencyEdge.State.Derived) {
                if(cVisited.contains(head.txnId)){
                    VertexId overHeadId = overVertexMap.get(head.txnId);
                    VertexId underHeadId = underVertexMap.get(head.txnId);

                    if(underHeadId.id != underSourceId.id){//二者连通但不属于同一个集合
                        for (VertexId l : reUnderVertexMap.get(overHeadId)) {
                            l.setId(overSourceId.id);
                        }
                    }

                    if (overHeadId.id != overSourceId.id) {//二者连通但不属于同一个集合,让head的source的Id
                        for (VertexId l : reOverVertexMap.get(overHeadId)) {
                            l.setId(overSourceId.id);
                        }
                        Set<VertexId> mergeVertex = reOverVertexMap.get(overSourceId);
                        mergeVertex.addAll(reOverVertexMap.remove(overHeadId));
                        reOverVertexMap.put(overSourceId,mergeVertex);
                    }
                }
                else if(dfs(head,overSourceId,underSourceId)){//该节点可以到达Target
                    Set<VertexId> mergeVertex = reOverVertexMap.get(overSourceId);
                    VertexId overHeadId = overVertexMap.get(head.txnId);
                    mergeVertex.add(overHeadId);
                }
            }else{

            }





        }

        return reach;

    }
}
