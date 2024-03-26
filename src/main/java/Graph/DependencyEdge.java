package Graph;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;

import javax.swing.plaf.PanelUI;
import java.util.ArrayList;

public class DependencyEdge extends DefaultWeightedEdge {
    public static enum Type{
        TO,WR,RW,WW
    }
    private boolean TO=false;
    private boolean WR=false;
    private boolean RW=false;
    private boolean WW=false;
    private boolean determinate=false;
    public ArrayList<WritePair> writePairs;

    // 构造函数
    public DependencyEdge(Type type) {
        setType(type);
        writePairs = new ArrayList<>();
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
