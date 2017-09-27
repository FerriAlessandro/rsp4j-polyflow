package it.polimi.jasper.engine.reasoning.pellet;


import it.polimi.jasper.engine.instantaneous.JenaGraph;
import it.polimi.jasper.engine.reasoning.InstantaneousInfGraph;
import it.polimi.jasper.engine.reasoning.JenaTVGReasoner;
import it.polimi.yasper.core.reasoning.TVGReasoner;
import openllet.jena.PelletInfGraph;
import openllet.jena.PelletReasoner;
import openllet.jena.graph.loader.DefaultGraphLoader;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerException;

/**
 * Created by riccardo on 06/07/2017.
 */
public class TVGReasonerPellet extends PelletReasoner implements JenaTVGReasoner, Reasoner {

    @Override
    public Reasoner bindSchema(Model model) throws ReasonerException {
        return super.bindSchema(model);
    }

    @Override
    public PelletInfGraph bind(Graph graph) throws ReasonerException {
        _logger.fine("In bind!");
        return new PelletInfTVGraph(graph, this, new DefaultGraphLoader(), null, -1);
    }


    public TVGReasoner bindSchema(JenaGraph data) {
        return bindSchema(data);
    }

    @Override
    public InstantaneousInfGraph bind(JenaGraph data) {
        return bind(data);
    }
}
