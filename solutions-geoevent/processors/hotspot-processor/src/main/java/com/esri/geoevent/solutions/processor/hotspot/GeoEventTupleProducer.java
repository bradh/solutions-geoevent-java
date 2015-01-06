package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.ges.core.geoevent.FieldCardinality;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;

public class GeoEventTupleProducer {
	private GeoEvent event;
	private GeoEventDefinition eventDef;
	private List<FieldDefinition> fieldDefs;
	private Map<String, Object> eventMap = new HashMap<String, Object>();
	private MapGeometry eventGeo;
	private String trackId;
	
	GeoEventTupleProducer(GeoEvent evt){
		event=evt;
		eventDef=event.getGeoEventDefinition();
		fieldDefs= eventDef.getFieldDefinitions();
		String guid = eventDef.getGuid();
		String defName = eventDef.getName();
		String owner = eventDef.getOwner();
		Map<String, Object> valueMap = new HashMap<String, Object>();
		eventGeo = event.getGeometry();
		trackId = event.getTrackId();
		
		eventMap.put("EventDefId", guid);
		eventMap.put("EventDefName", defName);
		eventMap.put("Owner", owner);
		eventMap.put("TrackId", trackId);
		eventMap.put("ValueMap", valueMap);
		String json = null;
		String geoType = null;
		if(eventGeo != null)
		{
			json = GeometryEngine.geometryToJson(eventGeo.getSpatialReference(), eventGeo.getGeometry());
			geoType = GetGeoTypeString(eventGeo);
		}
		eventMap.put("Geometry", json);
		eventMap.put("geoType", geoType);
		writeToMap(valueMap);
		
		
	}
	
	private String GetGeoTypeString(MapGeometry geo)
	{
		Type t = geo.getGeometry().getType();
		String type = null;
		if(t == Type.MultiPoint)
		{
			type = "multipoint";
		}
		else if(t == Type.Point)
		{
			type = "point";
		}
		else if(t == Type.Polyline)
		{
			type = "polyline";
		}
		else if(t == Type.Polygon)
		{
			type = "polygon";
		}
		else if(t == Type.Envelope)
		{
			type = "envelope";
		}
		else if(t == Type.Line)
		{
			type = "Line";
		}
		else if(t == Type.Unknown)
		{
			type = "unknown";
		}
		return type;
	}
	
	public GeoEvent getEvent()
	{
		return event;
	}
	
	public Map<String, Object> getEventMap()
	{
		return eventMap;
	}

	private void writeToMap(Map<String, Object> map)
	{
		
		for(FieldDefinition fd : fieldDefs)
		{
			writeItemToMap(map, fd);
		}
	}
	@SuppressWarnings("unchecked")
	private void writeItemToMap(Map<String, Object> map, FieldDefinition fd)
	{
		
		FieldType type = fd.getType();
		String name = fd.getName();
		FieldCardinality cardinality = fd.getCardinality();
		if (type == FieldType.Group) {

			HashMap<String, Object> childMap = new HashMap<String, Object>();
			map.put(name, childMap);
			List<FieldDefinition> children = fd.getChildren();
			for (FieldDefinition child : children) {
				writeItemToMap(childMap, child);
			}

		}
		else
		{
			if(cardinality == FieldCardinality.Many)
			{
				List<Object> objList = (List<Object>)event.getField(name);
				List<HashMap<String, Object>> listMap = new ArrayList<HashMap<String, Object>>();
				
				for(Object o: objList)
				{
					HashMap<String, Object> item = new HashMap<String, Object>();
					item.put("type", type);
					item.put("value", o);
					listMap.add(item);
				}

			} else {
				Object value = event.getField(name);
				writeItemToMap(map, name, type, value);
			}
		}
	}
	
	private void writeItemToMap(Map<String, Object> map, String name, FieldType type, Object value)
	{
		HashMap<String, Object> item = new HashMap<String, Object>();
		if(type==FieldType.Geometry)
		{
			MapGeometry mapGeo = (MapGeometry)value;
			Boolean isEventGeo = false;
			if(mapGeo.equals(eventGeo))
			{
				isEventGeo=true;
			}
			String json = GeometryEngine.geometryToJson(mapGeo.getSpatialReference(), mapGeo.getGeometry());
			item.put("type", type);
			item.put("value", json);
			item.put("isGeometry", isEventGeo);
			map.put(name, item);
		}
		else
		{
			
			
			item.put("type", type);
			item.put("value", value);
			map.put(name,  item);
			
		}
	}
	
	
	
}
