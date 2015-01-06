package com.esri.geoevent.solutions.processor.hotspot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;

import com.esri.arcgis.geometry.Polygon;
import com.esri.core.geometry.Geometry;
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

public class FindPointsInCellBolt extends BaseRichBolt {
	OutputCollector collector;
	private static final Log LOG = LogFactory.getLog(FindPointsInCellBolt.class);
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		String trackId = tuple.getStringByField("trackId");
		String json = tuple.getStringByField("point");
		Integer numCells tuple.getIntegerByField("numCells");
		MapGeometry mapGeo = null;
		try {
			mapGeo = GeometryEngine.jsonToGeometry(json);
		} catch (JsonParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Point p = (Point)mapGeo.getGeometry();
		SpatialReference sr = mapGeo.getSpatialReference();
		
		HashMap<String, String> cells = (HashMap<String, String>)tuple.getValueByField("cells");
		
		Set<String> keys = cells.keySet();
		Iterator<String> it = keys.iterator();
		Boolean overlaps = false;
		String k = null;
		Geometry cell = null;
		String cellJson = null;
		while(!overlaps)
		{
			k = it.next();
			cellJson = cells.get(k);
			
			try {
				cell = GeometryEngine.jsonToGeometry(cellJson).getGeometry();
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			overlaps = GeometryEngine.overlaps(p, cell, sr);
		}
		
		collector.emit(new Values(procId, k, cellJson, trackId, json));
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector=collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("procId", "cellId", "cellGeometry", "trackId", "point"));

	}

}
