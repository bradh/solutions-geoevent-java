package com.esri.geoevent.solutions.processor.hotspot;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.esri.ges.core.geoevent.GeoEvent;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class GeoEventInputSpout extends BaseRichSpout {
	
	private  LinkedBlockingQueue<GeoEvent>queue;
	private SpoutOutputCollector collector;

	GeoEventInputSpout()
	{
		
	}

	@Override
	public void nextTuple() {
		GeoEvent event = queue.poll();
		if(event != null)
		{
			GeoEventTupleProducer tupleProducer = new GeoEventTupleProducer(event);
			Map<String, Object> map = tupleProducer.getEventMap();
			this.collector.emit(new Values(map));
		}
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {

		queue = new LinkedBlockingQueue<GeoEvent>();
		this.collector = collector;	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("event"));
		
	}
	
	public void pushEvent(GeoEvent event)
	{
		queue.add(event);
	}

}
