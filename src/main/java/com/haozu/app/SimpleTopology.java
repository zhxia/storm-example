package com.haozu.app;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class SimpleTopology
{
    public static void main( String[] args ) throws Exception
    {
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("simple-spout", new SimpleSpout(),1);
        topologyBuilder.setBolt("simple-bilt",new SimpleBolt1(), 3).shuffleGrouping("simple-spout");
        topologyBuilder.setBolt("wordcounter", new SimpleBolt2(), 3).fieldsGrouping("simple-bilt", new Fields("info"));
        Config config=new Config();
        config.setDebug(true);
        if(null!=args&&args.length>0){
            //使用集群模式运行
            config.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }
        else{
            //使用本地模式运行
            config.setMaxTaskParallelism(1);
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("simple", config, topologyBuilder.createTopology());
        }
    }
}
