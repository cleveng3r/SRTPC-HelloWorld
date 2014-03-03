package storm.cookbook;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Hello World {@link backtype.storm.topology.base.BaseRichBolt Bolt}.
 *
 * @author Nate Clevenger (nc014668)
 */
public class HelloWorldBolt extends BaseRichBolt {
    private int myCount;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {
        String test = tuple.getStringByField("greeting");
        if ("Hello World!".equals(test)) {
            ++myCount;
            System.out.println("Found a Hello World!  My Count is now [" + Integer.toString(myCount) + "]");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
