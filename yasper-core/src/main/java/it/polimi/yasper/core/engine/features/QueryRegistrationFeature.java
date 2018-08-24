package it.polimi.yasper.core.engine.features;

import it.polimi.yasper.core.rspql.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.rspql.querying.ContinuousQuery;
import it.polimi.yasper.core.rspql.querying.QueryConfiguration;

public interface QueryRegistrationFeature<Q extends ContinuousQuery> {

    ContinuousQueryExecution register(Q q);

    ContinuousQueryExecution register(Q q, QueryConfiguration c);

}
