package storm.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Hello World Topology.
 *
 * @author Nate Clevenger (nc014668)
 */
public class HelloWorldTopology {
    /**
     * Main method.
     * @param args command arguments
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        String spoutName = "randomHelloWorld";
        String boltName = "HelloWorldBolt";

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutName, new HelloWorldSpout(), 10);
        builder.setBolt(boltName, new HelloWorldBolt(), 2)
            .shuffleGrouping(spoutName);

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
