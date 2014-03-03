package storm.cookbook;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Hello World {@link backtype.storm.topology.base.BaseRichSpout Spout}.
 *
 * @author Nate Clevenger (nc014668)
 */
public class HelloWorldSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private int referenceRandom;
    private static final int MAX_RANDOM = 10;

    public HelloWorldSpout() {
        final Random rand = new Random();
        referenceRandom = rand.nextInt(MAX_RANDOM);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("greeting"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        if (rand.nextInt(MAX_RANDOM) == referenceRandom) {
            collector.emit(new Values("Hello World!"));
        } else {
            collector.emit(new Values("Go Away!"));
        }
    }
}
