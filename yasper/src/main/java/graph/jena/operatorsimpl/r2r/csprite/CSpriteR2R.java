package graph.jena.operatorsimpl.r2r.csprite;

import graph.jena.datatypes.JenaGraphOrBindings;
import graph.jena.operatorsimpl.r2r.jena.BGP;
import graph.jena.operatorsimpl.r2r.jena.TP;
import org.apache.jena.vocabulary.RDF;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CSpriteR2R implements RelationToRelationOperator<JenaGraphOrBindings> {
    private final HierarchySchema hierarchySchema;
    private final RelationToRelationOperator<JenaGraphOrBindings> r2r;
    private final UpwardExtension upwardExtension;
    private final R2RUpwardExtension r2rUpward;
    private Set<String> queriedTypes;
    private Set<String> queriedProperties;

    public CSpriteR2R(RelationToRelationOperator<JenaGraphOrBindings> relationOperator, HierarchySchema hierarchySchema) {
        this.r2r = relationOperator;
        this.hierarchySchema = hierarchySchema;
        this.queriedTypes = new HashSet<>();
        this.queriedProperties = new HashSet<>();
        upwardExtension = new UpwardExtension(hierarchySchema.getSchema());
        this.findTypes();
        this.pruneHierarchy();
        this.r2rUpward = new R2RUpwardExtension(this.upwardExtension);
    }

    public void findTypes() {
        if (r2r instanceof TP) {
            TP tp = (TP) r2r;
            extractTPTypes(tp);

        } else if (r2r instanceof BGP) {
            BGP bgp = (BGP) r2r;
            bgp.getTPs().forEach(tp -> extractTPTypes(tp));
        }
    }

    private void extractTPTypes(TP tp) {
        if (RDF.type.asNode().equals(tp.getProperty()) && tp.getObject().isURI()) {
            queriedTypes.add(tp.getObject().getURI());
        }
        if (RDF.type.asNode().equals(tp.getProperty()) && tp.getObject().isVariable()) { // all types are being queried
            List<String> childs = hierarchySchema.getSchema().values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            queriedTypes.addAll(childs);
            queriedTypes.addAll(hierarchySchema.getSchema().keySet());
        }
    }

    public void pruneHierarchy() {
        Map<String, Set<String>> upwardExtensions = upwardExtension.getExtensions();
        Map<String, Set<String>> result =
                upwardExtensions.entrySet().stream()
                        .filter(
                                e -> {
                                    e.getValue().retainAll(this.queriedTypes);
                                    return !e.getValue().isEmpty();
                                })
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        upwardExtension.setExtensions(result);
    }

    public Map<String, Set<String>> getHierachy() {
        return upwardExtension.getExtensions();
    }

    @Override
    public JenaGraphOrBindings eval(List<JenaGraphOrBindings> datasets) {
        return r2rUpward.eval(datasets);
    }

    @Override
    public List<String> getTvgNames() {
        return null;
    }

    @Override
    public String getResName() {
        return null;
    }
    public Set<String> getQueriedTypes() {
        return queriedTypes;
    }

}
