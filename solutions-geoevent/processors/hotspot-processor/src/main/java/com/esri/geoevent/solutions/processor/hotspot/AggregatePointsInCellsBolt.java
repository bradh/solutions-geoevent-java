package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.DefaultGeoEventDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AggregatePointsInCellsBolt extends BaseRichBolt {
	private static final Log LOG = LogFactory.getLog(AggregatePointsInCellsBolt.class);
	private OutputCollector collector;
	HashMap<String, HashMap<String, HashMap<String, Object>>> cellCountByProc;
	
	AggregatePointsInCellsBolt()
	{
		
	}
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		String cellId = tuple.getStringByField("cellId");
		Integer numCells = tuple.getIntegerByField("numCells");
		String trackId = tuple.getStringByField("trackId");
		String cellGeo = tuple.getStringByField("cellGeometry");
		String point = tuple.getStringByField("point");
		HashMap<String, HashMap<String, Object>> cellProperties;
		HashMap<String, Object>properties;
		if(!cellCountByProc.containsKey(procId))
		{
			Long count = (long) 1; 
			cellProperties = new HashMap<String, HashMap<String, Object>>();
			properties = new HashMap<String, Object>();
			properties.put("count", count);
			properties.put("geometry", cellGeo);
			cellProperties.put(cellId, properties);
			cellCountByProc.put(procId, cellProperties);
		}
		else
		{
			cellProperties = cellCountByProc.get(procId);
			if(!cellProperties.containsKey(cellId))
			{
				Long count = (long) 1; 
				cellProperties = new HashMap<String, HashMap<String,Object>>();
				properties = new HashMap<String, Object>(); 
				properties.put("count", count);
				properties.put("geometry", cellGeo);
				cellProperties.put(cellId, properties);
				cellCountByProc.put(procId, cellProperties);
			}
			else
			{
				
				properties = cellProperties.get(cellId);
				Long count = (Long)properties.get("count");
				count +=1;
				properties.put("count", count);
				cellProperties.put(cellId, properties);
				cellCountByProc.put(procId, cellProperties);
			}
		}
		if(cellProperties.size()==numCells)
		{
			
			Set<String>keys = cellProperties.keySet();
			Iterator<String> it = keys.iterator();
			for(String k: keys){
				properties = cellProperties.get(k);
				Long count = (Long)properties.get("count");
				String geo = (String)properties.get("geometry");
				//create geoevent ... pass it to processor
			}
		}
	
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		cellCountByProc = new HashMap<String,HashMap<String, HashMap<String, Object>>>();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());;

	}
	
	private GeoEvent createEvent(String procId, String cellId, Long count, String geo) throws ConfigurationException
	{
		GeoEventDefinition ged = new DefaultGeoEventDefinition();
		List<FieldDefinition> fldDefs = new ArrayList<FieldDefinition>();
		FieldDefinition idFd = new DefaultFieldDefinition("CellId", FieldType.String, "TRACK_ID");
		fldDefs.add(idFd);
		FieldDefinition procFd = new DefaultFieldDefinition("ProcId", FieldType.String, "PROCESS_ID");
		fldDefs.add(procFd);
		FieldDefinition countFd = new DefaultFieldDefinition("count", FieldType.Integer);
		fldDefs.add(countFd);
		FieldDefinition geoFd = new DefaultFieldDefinition("geometry", FieldType.Geometry, "GEOMETRY");
		fldDefs.add(geoFd);
		ged.setFieldDefinitions(fldDefs);
		GeoEvent ge = null;
		return ge;
		
	}

}
