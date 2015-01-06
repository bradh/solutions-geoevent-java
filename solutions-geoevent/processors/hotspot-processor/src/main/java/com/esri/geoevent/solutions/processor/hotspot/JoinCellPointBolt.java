package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinCellPointBolt extends BaseRichBolt {
	private OutputCollector collector;
	private static final Log LOG = LogFactory.getLog(JoinCellPointBolt.class);
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		HashMap<String,String> points = (HashMap<String, String>) tuple.getValueByField("points");
		Integer numCells = (Integer)tuple.getIntegerByField("numCells");
		HashMap<String,String> cells = (HashMap<String, String>)tuple.getValueByField("cellList");
		Set<String> keys = points.keySet();
		for(String k: keys)
		{
			String p = points.get(k);
			collector.emit(new Values(procId, k, p, numCells, cells));
		}

	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("procId", "trackId", "point", "numCells", "cells"));

	}

}
