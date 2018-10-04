package ee.ut.cs.dsg.windowingsemantics;

import ee.ut.cs.dsg.windowingsemantics.bolts.SlidingWindowBolt;
import ee.ut.cs.dsg.windowingsemantics.spouts.FileSpout;
import ee.ut.cs.dsg.windowingsemantics.timestampextractor.MyTimestampExtractor;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.concurrent.TimeUnit;

public class Runner {

    public enum WindowType
    {
        Sliding,
        Tumbling
    }
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FileSpout(), 1);

        //BaseWindowedBolt windowBolt = createWindowBolt(0,0,5,2,0,WindowType.Sliding);

        BaseWindowedBolt windowBolt = createWindowBolt(0,3,5,0,0,WindowType.Sliding);
        builder.setBolt("slidingwindowbolt", windowBolt,1).shuffleGrouping("spout");
        Config conf = new Config();
//        conf.put("inputFile", "DummyDataWithoutDelays.csv");
        conf.put("inputFile", "DataRealShort.csv");
        conf.setDebug(true);
        conf.setNumWorkers(1);

        LocalCluster localCluster  = new LocalCluster();

        localCluster.submitTopology("testToplogy",conf, builder.createTopology());
//        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

      //  localCluster.shutdown();
    }

    private static BaseWindowedBolt createWindowBolt(int countWindowWidth, int countWindowSlide, int timeWindowWidth, int timeWindowSlide, int lag, WindowType winType) throws Exception {

        if (!(countWindowWidth > 0 ^ timeWindowWidth > 0))
        {
            throw new Exception("Exactly one of the countWindowWidth or timeWindowWidth can be greater than zero at a time");
        }
        if (!(countWindowSlide > 0 ^ timeWindowSlide > 0) && winType == WindowType.Sliding)
        {
            throw new Exception("Exactly one of the countWindowSlide or timeWindowSlide can be greater than zero at a time");
        }
        BaseWindowedBolt windowBolt = new SlidingWindowBolt();

        if (winType == WindowType.Sliding) {
            if (countWindowWidth > 0 && countWindowSlide > 0) {
                windowBolt = windowBolt.withWindow(new BaseWindowedBolt.Count(countWindowWidth), new BaseWindowedBolt.Count(countWindowSlide));
            }
            else if (countWindowWidth > 0 && timeWindowSlide > 0) {
                windowBolt = windowBolt.withWindow(new BaseWindowedBolt.Count(countWindowWidth), new BaseWindowedBolt.Duration(timeWindowSlide, TimeUnit.MILLISECONDS));
            }
            else if (timeWindowWidth > 0 && timeWindowSlide > 0){
                windowBolt = windowBolt.withWindow(new BaseWindowedBolt.Duration(timeWindowWidth, TimeUnit.MILLISECONDS), new BaseWindowedBolt.Duration(timeWindowSlide, TimeUnit.MILLISECONDS));
            }
            else
            {
                windowBolt = windowBolt.withWindow(new BaseWindowedBolt.Duration(timeWindowWidth, TimeUnit.MILLISECONDS),new BaseWindowedBolt.Count(countWindowSlide));
            }
        }
        else
        {
            if (countWindowWidth > 0){
                windowBolt = windowBolt.withTumblingWindow(new BaseWindowedBolt.Count(countWindowWidth));
            }
            else if (timeWindowWidth > 0)
            {
                windowBolt = windowBolt.withTumblingWindow(new BaseWindowedBolt.Duration(timeWindowWidth, TimeUnit.MILLISECONDS));
            }

        }
        windowBolt = windowBolt.withTimestampExtractor(new MyTimestampExtractor()).withLag(new BaseWindowedBolt.Duration(lag, TimeUnit.MILLISECONDS));
        windowBolt = windowBolt.withWatermarkInterval(new BaseWindowedBolt.Duration(5000, TimeUnit.SECONDS));
        return windowBolt;
    }
}
