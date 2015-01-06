package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GenerateFishnetBolt extends BaseRichBolt {
	private static final Log LOG = LogFactory.getLog(GenerateFishnetBolt.class);
	private OutputCollector collector;
	private Map<String, HashMap<String, String>> cellMapByProc;
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		Integer numCols = tuple.getIntegerByField("numCols");
		Integer numRows = tuple.getIntegerByField("numCols");
		String json = tuple.getStringByField("cell");
		HashMap<String,String> cellMap = null;
		Integer numCells = numCols*numRows;
		String cellid = UUID.randomUUID().toString();
		if(!cellMap.containsKey(procId))
		{
			cellMap = new HashMap<String, String>();
			
			cellMap.put(cellid, json);
			cellMapByProc.put(procId, cellMap);
		}
		else
		{
			cellMap = cellMapByProc.get(procId);
			cellMap.put(cellid, json);
		}
		
		if (cellMap.size() == numCells)
		{
			collector.emit(new Values(procId, numCells, cellMap));
		}
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.cellMapByProc = new HashMap<String, HashMap<String, String>>();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("procId", "numCells", "cellList"));

	}

}
