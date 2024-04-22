package Graph.Edge;

import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.ArrayList;
//Weight= writePairs.size() :determinate
//Weight = savePoint 10000 -- undetermined
public class DependencyEdge extends Edge {
    public static enum Type{
        TO,WR,RW,WW
    }
<<<<<<< HEAD
<<<<<<< HEAD
    public static enum State{
        Determinate,Derived,Undetermined
    }
=======
=======
>>>>>>> main
    private State state = State.Derived;
    public void setState(State state){
        this.state = state;
    }
    public static enum State{
        Determinate,Derived,Undetermined
    }
    public State getState(){
        return this.state;
    }
    public final static int Large = 100000;
<<<<<<< HEAD
>>>>>>> 676b501 (s)
=======
>>>>>>> main
    private boolean TO=false;
    private boolean WR=false;
    private boolean RW=false;
    private boolean WW=false;
    private boolean determinate=false;
    private State state = State.Undetermined;
    public ArrayList<WritePair> writePairs;
    public ArrayList<DependencyEdge> affectEdges;

    // 构造函数
    public DependencyEdge(Type type) {
        setType(type);
        writePairs = new ArrayList<>();
    }
    public State getState(){
        return this.state;
    }
    public void BackTrace(boolean determinate){

    }
    public void setType(Type type){
        switch (type){
            case TO:
                this.determinate = true;
                this.state = State.Determinate;
                this.TO = true;
                break;
            case WR:
                this.WR =true;
                break;
            case RW:
                this.RW = true;
                this.determinate = true;
                this.state = State.Determinate;
                break;
            case WW:
                this.WW = true;
                break;
            default:
                break;
        }
    }
    public boolean isRW(){
        return this.RW;
    }
    public boolean isWR(){
        return this.WR;
    }
    public boolean isWW(){
        return this.WW;
    }
    public boolean isTO(){
        return this.TO;
    }
    public boolean isDeterminate(){
        return this.determinate;
    }
    public void setDeterminate(boolean determinate){
        this.determinate = determinate || this.determinate;
        if(determinate){
            for(DependencyEdge e:affectEdges){
                for(WritePair wp:e.writePairs){
                    wp.checkEdge(this);
                }
            }
        }
    }
}