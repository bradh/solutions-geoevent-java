package com.esri.geoevent.solutions.processor.hotspot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

//import com.esri.ges.core.component.ComponentDefinition;
//import com.esri.ges.core.geoevent.GeoEvent;
//import com.esri.ges.core.geoevent.GeoEventDefinition;
//import com.esri.ges.processor.GeoEventProcessorDefinition;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class GeoEventInputSpout extends BaseRichSpout {
	
	private  LinkedBlockingQueue<String>queue;
	private SpoutOutputCollector collector;
	private Map<String, String>fields = null;
	//private GeoEventProcessorDefinition definition;
	
	GeoEventInputSpout()
	{
		
	}
	GeoEventInputSpout(Map<String, String> fields)
	{
		this.fields = fields;
	}
	@Override
	public void nextTuple() {
		String event = queue.poll();
		if(event != null)
		{
			GeoEventTupleProducer tupleProducer = new GeoEventTupleProducer(event);
			Map<String, Object> eventMap = tupleProducer.getEventMap();
			String trackId = (String)eventMap.get("TrackId");
			String json = (String)eventMap.get("Geometry");
			String geoType = (String)eventMap.get("geoType");
			String owner = (String)eventMap.get("owner");
			HashMap<String, Object> valueMap = (HashMap<String, Object>)eventMap.get("ValueMap");
			
			this.collector.emit(new Values(trackId, geoType, json, owner, valueMap));
		}
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {

		queue = new LinkedBlockingQueue<String>();
		this.collector = collector;	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields flds = new Fields();
		
		
		declarer.declare(new Fields("trackId", "geoType", "geometry", "owner", "valuemap"));
		
		
	}
	
	public void pushEvent(String event)
	{
		queue.add(event);
	}
	
	//public void setGeoEventDefinition(GeoEventProcessorDefinition definition)
	//{
		//this.definition = definition;
	//}

}
