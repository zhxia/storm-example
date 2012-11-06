package com.haozu.app;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt2 extends BaseBasicBolt {

    /**
     *
     */
    private static final long serialVersionUID = 2246728833921545676L;
    Integer id;
    String name;
    Map<String, Integer> counters;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters=new HashMap<String, Integer>();
        this.name=context.getThisComponentId();
        this.id=context.getThisTaskId();
        System.out.println(String.format("componentId:%s",this.name));
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word=input.getString(0);
        if(counters.containsKey(word)){
            Integer c=counters.get(word);
            counters.put(word, c+1);
        }
        else{
            counters.put(word, 1);
        }
        collector.emit(new Values(word,counters.get(word)));
        System.out.println(String.format("stats result is:%s:%s", word,counters.get(word)));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

}
