package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.core.geometry.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExtractGeometryBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private static final Log LOG = LogFactory.getLog(ExtractGeometryBolt.class);
	//Map<String, Point> eventGeometries = new HashMap<String, Point>();
	
	@Override
	public void execute(Tuple tuple) {
		Map<String, Object> eventMap = (Map<String, Object>)tuple.getValueByField("event");
		String trackId = (String)eventMap.get("TrackId");
		String json = (String)eventMap.get("Geometry");
		String geoType = (String)eventMap.get("geoType");
		
		this.collector.emit(new Values(trackId, json, geoType));
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("TrackId","geometry","geoType"));

	}

}
