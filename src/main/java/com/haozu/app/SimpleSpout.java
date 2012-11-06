package com.haozu.app;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SimpleSpout extends BaseRichSpout{

    /**
     *
     */
    private static final long serialVersionUID = -6335251364034714629L;
    private SpoutOutputCollector collector;
    private static String[] info=new String[]{
        "hello",
        "world",
        "what",
        "when",
        "where",
        "who",
        "why"
    };
    Random random=new Random();
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("source"));
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
            this.collector=collector;
    }

    public void nextTuple() {
       try{
       String msg=info[random.nextInt(info.length)];
       collector.emit(new Values(msg));
       Thread.sleep(1000);
       }
       catch(InterruptedException e){
           e.printStackTrace();
       }

    }

}
