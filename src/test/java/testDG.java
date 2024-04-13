import Graph.*;
import org.checkerframework.checker.units.qual.A;
import org.jgrapht.graph.DirectedWeightedPseudograph;
import org.jgrapht.graph.WeightedMultigraph;
import org.jgrapht.util.SupplierUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class testDG {
    DependencyGraph g = new DependencyGraph(null, SupplierUtil.createSupplier(DependencyEdge.class),true,false);
    //DependencyGraph g = new DependencyGraph(DependencyEdge.class);
    public ArrayList<TransactionLT> ver = new ArrayList<>();
    public testDG(){
        for(int i=1;i<=8;i++){
            TransactionLT vertex =  new TransactionLT(i,0);
            ver.add(vertex);
            g.addVertex(vertex);
        }
        g.addEdge(ver.get(0),ver.get(1),new DependencyEdge(DependencyEdge.Type.WW));
        g.addEdge(ver.get(1),ver.get(2),new DependencyEdge(DependencyEdge.Type.TO));
        g.addEdge(ver.get(2),ver.get(3),new DependencyEdge(DependencyEdge.Type.TO));
        g.addEdge(ver.get(3),ver.get(4),new DependencyEdge(DependencyEdge.Type.TO));


        g.addEdge(ver.get(0),ver.get(5),new DependencyEdge(DependencyEdge.Type.TO));
        g.addEdge(ver.get(5),ver.get(6),new DependencyEdge(DependencyEdge.Type.TO));
        g.addEdge(ver.get(6),ver.get(7),new DependencyEdge(DependencyEdge.Type.TO));
        g.addEdge(ver.get(7),ver.get(4),new DependencyEdge(DependencyEdge.Type.WW));

        g.addEdge(ver.get(1),ver.get(6),new DependencyEdge(DependencyEdge.Type.WW));
        g.addEdge(ver.get(6),ver.get(3),new DependencyEdge(DependencyEdge.Type.WW));

    }
    public static void main(String[] args) {
        testDG t = new testDG();
        GraphReduce gr = new GraphReduce(t.g,t.ver.get(0),t.ver.get(4));
        SupportGraph over=null,under = null;
        gr.getSupportGraph(over,under);
        List<List<DependencyEdge>> dps = new ArrayList<>();
        boolean isd = t.g.checkEdge(t.ver.get(4),t.ver.get(0),dps);
        dps.sort(Comparator.comparing(List::size));
        for(List<DependencyEdge> path :dps){
            System.out.println("Path");
            for(DependencyEdge edge :path){
                System.out.print("w"+ t.g.getEdgeWeight(edge));
                System.out.print(t.g.getEdgeSource(edge).txnId);
                System.out.print("-");
                System.out.print(t.g.getEdgeTarget(edge).txnId);
                System.out.print(". ");
            }
            System.out.println("D");
        }
        System.out.println(isd);
    }
}

