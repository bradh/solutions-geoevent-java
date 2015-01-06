package com.esri.geoevent.solutions.processor.hotspot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GenerateNearestNeighborBolt extends BaseRichBolt {
	private OutputCollector collector;
	private static final Log LOG = LogFactory.getLog(GenerateNearestNeighborBolt.class);
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		String trackId = tuple.getStringByField("trackId");
		String currentPtString = tuple.getStringByField("current");
		Integer size = tuple.getIntegerByField("size");
		HashMap<String, String>points = (HashMap<String, String>)tuple.getValueByField("points");
		
		MapGeometry mapGeo = null;
		SpatialReference sr = null;
		try {
			mapGeo = GeometryEngine.jsonToGeometry(currentPtString);
			sr = mapGeo.getSpatialReference();
		} catch (JsonParseException e) {
			LOG.error("JSON Parse Error: error parsing json string");
			LOG.error(e.getStackTrace());
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error("IO Error: cannot parse. Input invalid");
			LOG.error(e.getStackTrace());
			LOG.error(e.getMessage());
		}
		
		Double nearest = -1.0;
		Point p = (Point)mapGeo.getGeometry();
		Set<String> keys = points.keySet();
		
		for (String k:keys)
		{
			String comparePt = points.get(k);
			MapGeometry geo = null;
			try {
				geo = GeometryEngine.jsonToGeometry(comparePt);
			} catch (JsonParseException e) {
				LOG.error("JSON Parse Error: error parsing json string");
				LOG.error(e.getStackTrace());
				LOG.error(e.getMessage());
			} catch (IOException e) {
				LOG.error("IO Error: cannot parse. Input invalid");
				LOG.error(e.getStackTrace());
				LOG.error(e.getMessage());
			}
			Point q = (Point)geo.getGeometry();
			if (!k.equals(trackId)) {
				Boolean disjoint = GeometryEngine.disjoint(p,q,sr);
				if (disjoint) {
					Double d = GeometryEngine.distance(p,q,sr);
					if (nearest < 0) {
						nearest = d;
					} else {
						if (d < nearest)
						{
							nearest=d;
						}
					}
				}
			}
		}
		//geoMap.put("nn", nearest);
		collector.emit(new Values(procId,trackId,currentPtString,size,nearest));
		

	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("procId","trackId","current","size","nn"));

	}

}
