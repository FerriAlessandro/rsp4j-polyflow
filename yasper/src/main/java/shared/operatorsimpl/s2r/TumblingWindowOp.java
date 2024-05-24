package shared.operatorsimpl.s2r;

import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVaryingFactory;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.time.Time;

public class TumblingWindowOp<I, W, R extends Iterable<?>> extends HoppingWindowOp<I, W, R> {
    public TumblingWindowOp(Tick tick, Time time, String name, ContentFactory<I, W, R> cf, TimeVaryingFactory<R> tvFactory, ReportGrain grain, Report report, long width) {
        super(tick, time, name, cf, tvFactory, grain, report, width, width);
    }
}
