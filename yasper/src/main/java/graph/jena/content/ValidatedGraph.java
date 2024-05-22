package graph.jena.content;

import org.apache.jena.graph.Graph;

public class ValidatedGraph {

    public Graph report;
    public Graph content;

    public ValidatedGraph(Graph report, Graph content){
        this.report = report;
        this.content = content;
    }

    public Graph getReport(){
        return this.report;
    }

    public Graph getContent(){
        return this.content;
    }

}
