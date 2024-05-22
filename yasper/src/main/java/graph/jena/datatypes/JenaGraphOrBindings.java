package graph.jena.datatypes;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class JenaGraphOrBindings implements Iterable<Binding> {


    private Graph content;
    private List<Binding> result;

    public JenaGraphOrBindings(Graph content) {
        this.content = content;
        this.result = new ArrayList<>();
    }

    public JenaGraphOrBindings() {
        this.result = new ArrayList<>();
        this.content = GraphFactory.createGraphMem();
    }

    @Override
    public Iterator<Binding> iterator() {
        return result.iterator();
    }

    @Override
    public void forEach(Consumer<? super Binding> action) {
        result.forEach(action);
    }


    public Graph getContent() {
        return this.content;
    }

    public List<Binding> getResult() {
        return this.result;
    }

    public void setContent(Graph content) {
        this.content = content;
    }

    public void setResult(List<Binding> res) {
        this.result = res;
    }
}
