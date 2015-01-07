package com.esri.geoevent.solutions.processor.hotspot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParseException;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.DefaultGeoEventDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AggregatePointsInCellsBolt extends BaseRichBolt {
	private static final Log LOG = LogFactory
			.getLog(AggregatePointsInCellsBolt.class);
	private OutputCollector collector;

	HashMap<String, HashMap<String, HashMap<String, Object>>> cellCountByProc;
	AggregatePointsInCellsBolt() {
	}

	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		String cellId = tuple.getStringByField("cellId");
		Integer numCells = tuple.getIntegerByField("numCells");
		String trackId = tuple.getStringByField("trackId");
		String cellGeo = tuple.getStringByField("cellGeometry");
		String point = tuple.getStringByField("point");
		String owner = tuple.getStringByField("owner");
		HashMap<String, HashMap<String, Object>> cellProperties;
		HashMap<String, Object> properties;
		if (!cellCountByProc.containsKey(procId)) {
			Long count = (long) 1;
			cellProperties = new HashMap<String, HashMap<String, Object>>();
			properties = new HashMap<String, Object>();
			properties.put("count", count);
			properties.put("geometry", cellGeo);
			cellProperties.put(cellId, properties);
			cellCountByProc.put(procId, cellProperties);
		} else {
			cellProperties = cellCountByProc.get(procId);
			if (!cellProperties.containsKey(cellId)) {
				Long count = (long) 1;
				cellProperties = new HashMap<String, HashMap<String, Object>>();
				properties = new HashMap<String, Object>();
				properties.put("count", count);
				properties.put("geometry", cellGeo);
				cellProperties.put(cellId, properties);
				cellCountByProc.put(procId, cellProperties);
			} else {

				properties = cellProperties.get(cellId);
				Long count = (Long) properties.get("count");
				count += 1;
				properties.put("count", count);
				cellProperties.put(cellId, properties);
				cellCountByProc.put(procId, cellProperties);
			}
		}
		if (cellProperties.size() == numCells) {
			GeoEventDefinition ged = null;
			

			Set<String> keys = cellProperties.keySet();
			Iterator<String> it = keys.iterator();
			for (String k : keys) {
				properties = cellProperties.get(k);
				Long count = (Long) properties.get("count");
				String geo = (String) properties.get("geometry");
				collector.emit(new Values(procId, k, owner, count, geo));
				
			}
		}

	}

	@Override
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		cellCountByProc = new HashMap<String, HashMap<String, HashMap<String, Object>>>();
		//queue = new LinkedBlockingQueue<GeoEvent>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("procId", "cellId", "owner", "count", "cellGeometry"));
	}




}
