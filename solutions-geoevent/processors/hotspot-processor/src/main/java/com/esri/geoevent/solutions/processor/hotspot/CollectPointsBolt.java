package com.esri.geoevent.solutions.processor.hotspot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CollectPointsBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String, String> points = new HashMap<String, String>();
	private static final Log LOG = LogFactory.getLog(CollectPointsBolt.class);
	private Integer interval;
	private String intervalType;
	private Boolean intervalIsTime=false;
	private Integer minsize = null;
	private Boolean timereset = true;
	private long ts;
	public CollectPointsBolt(String intervalType, Integer interval, Integer minsize)
	{
		this.interval=interval;
		this.intervalType=intervalType;
		this.minsize=minsize;
	}
	@Override
	public void execute(Tuple tuple) {
		String json = tuple.getStringByField("geometry");
		String geoType = tuple.getStringByField("geoType");
		String trackId = tuple.getStringByField("TrackId");
		String owner = tuple.getStringByField("owner");

		if (geoType.equals("point"))
		{
			points.put(trackId ,json);
			if (intervalIsTime) {
				if(timereset)
				{
					ts = System.currentTimeMillis();
				}
				long now = System.currentTimeMillis();
				if( now - ts >= interval )
				{
					timereset=true;
					if (points.size() >= minsize) {
						String procId = UUID.randomUUID().toString();
						Set<String> keys = points.keySet();
						for(String k:keys)
						{
							String currentPt = points.get(k);
							String id = k;
							Integer size = points.size();
							collector.emit("processStream",new Values(procId,id,currentPt,size,points));
							collector.emit("pointsStream",new Values(procId,points));
							collector.emit("ownerStream",new Values(procId,owner));
						}
					}
				}
			} else {
				if (points.size() > minsize && points.size() % interval == 0) {
					String procId = UUID.randomUUID().toString();
					Set<String> keys = points.keySet();
					for(String k:keys)
					{
						
						String currentPt = points.get(k);
						String id = k;
						Integer size = points.size();
						collector.emit("processStream",new Values(procId,id,currentPt,size,points));
						collector.emit("pointsStream",new Values(procId,points));
						collector.emit("ownerStream",new Values(procId, owner));
					}
					
				}
			}
		}
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		if(this.intervalType=="TIME")
		{
			intervalIsTime=true;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("pointsStream", new Fields("procId", "points"));
		declarer.declareStream("processStream",new Fields("procId","trackId","current","size","points"));

	}

}
