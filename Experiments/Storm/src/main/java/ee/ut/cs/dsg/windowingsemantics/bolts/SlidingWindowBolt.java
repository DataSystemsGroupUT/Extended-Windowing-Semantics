package ee.ut.cs.dsg.windowingsemantics.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public class SlidingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private TopologyContext context;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    //@Override
//    int cnt = 0;
    public void execute(TupleWindow inputWindow) {

        System.out.println("Elements removed since last trigger");
        for(Tuple tuple: inputWindow.getExpired()) {
            System.out.println(tuple.getStringByField("Timestamp"));
            System.out.println(tuple.getStringByField("Key"));
            System.out.println(tuple.getStringByField("Value"));
            System.out.println("----------------------------------");
        }

        System.out.println("Elements in the window");
        for(Tuple tuple: inputWindow.get()) {
            System.out.println(tuple.getStringByField("Timestamp"));
            System.out.println(tuple.getStringByField("Key"));
            System.out.println(tuple.getStringByField("Value"));
            System.out.println("----------------------------------");
        }

        System.out.println("Elements added since last trigger");
        for(Tuple tuple: inputWindow.getNew()) {
            System.out.println(tuple.getStringByField("Timestamp"));
            System.out.println(tuple.getStringByField("Key"));
            System.out.println(tuple.getStringByField("Value"));
            System.out.println("----------------------------------");
        }
//        String result = "("+inputWindow.get()+")";
//        System.out.println()
//        // emit the results
//        collector.emit(new Values(computedValue));
    }
}