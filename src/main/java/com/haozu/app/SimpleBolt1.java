package com.haozu.app;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt1 extends BaseBasicBolt {

    /**
     *
     */
    private static final long serialVersionUID = -5266922733759958473L;

    public void execute(Tuple input, BasicOutputCollector collector) {
        String message=input.getString(0);
        if(null!=message.trim()){
            collector.emit(new Values(message));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("info"));
    }

}
