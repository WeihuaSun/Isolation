package graph.edge;

import Verifier.writePair.WritePair;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.ArrayList;

public class DependencyEdge extends DefaultWeightedEdge {
    public static enum Type{
        TO,WR,RW,WW
    }
    public static enum State{
        Determinate,Derived,Undetermined
    }
    private boolean TO=false;
    private boolean WR=false;
    private boolean RW=false;
    private boolean WW=false;
    private boolean determinate=false;
    private State state = State.Undetermined;
    public ArrayList<WritePair> writePairs;

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
                this.TO = true;
                break;
            case WR:
                this.WR =true;
                break;
            case RW:
                this.RW = true;
                this.determinate = true;
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
    }
}
