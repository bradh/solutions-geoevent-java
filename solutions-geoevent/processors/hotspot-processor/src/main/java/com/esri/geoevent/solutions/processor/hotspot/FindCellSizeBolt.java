package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FindCellSizeBolt extends BaseRichBolt {
	private static final Log LOG = LogFactory.getLog(FindCellSizeBolt.class);
	private OutputCollector collector;
	Map<String, ArrayList<Double>> nnMap = null;
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		Integer size = tuple.getIntegerByField("size");
		Double nn = tuple.getDoubleByField("nn");
		ArrayList<Double> proc = null; 
		if(!nnMap.containsKey(procId))
		{
			proc = new ArrayList<Double>();
			proc.add(nn);
			nnMap.put(procId, proc);
		}
		else
		{
			proc = nnMap.get(procId);
			proc.add(nn);
		}
		if(proc.size() == size)
		{
			Double sum = 0.0;
			
			Integer count = 0;
			Integer medIndex = Math.floorDiv(size, 2);
			Double mnn = proc.get(medIndex);
			for(Double nearest: proc)
			{
				sum += nearest;
			}
			Double ann = sum/size;
			Double initCellSize = Math.max(ann, mnn);
			Double larger = initCellSize;
			Double smaller = Math.min(ann, mnn);
			Double scalar = Math.max(larger/smaller, 2);
			Double adjCellSize = initCellSize * scalar;
			collector.emit(new Values(procId, adjCellSize));

		}

	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		nnMap = new HashMap<String, ArrayList <Double>>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields( "procId", "cellsize"));

	}

}
