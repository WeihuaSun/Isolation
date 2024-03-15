package Graph;

import org.jgrapht.graph.DefaultEdge;

public class DependencyEdge extends DefaultEdge {
    private String label;

    // 构造函数
    public DependencyEdge(String label) {
        this.label = label;
    }

    // 获取边的标签
    public String getLabel() {
        return label;
    }

    // 设置边的标签
    public void setLabel(String label) {
        this.label = label;
    }
}
