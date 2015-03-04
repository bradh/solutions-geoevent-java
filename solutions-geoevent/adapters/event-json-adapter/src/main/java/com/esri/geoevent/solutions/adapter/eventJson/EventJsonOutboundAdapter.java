/*
 | Copyright 2013 Esri
 |
 | Licensed under the Apache License, Version 2.0 (the "License");
 | you may not use this file except in compliance with the License.
 | You may obtain a copy of the License at
 |
 |    http://www.apache.org/licenses/LICENSE-2.0
 |
 | Unless required by applicable law or agreed to in writing, software
 | distributed under the License is distributed on an "AS IS" BASIS,
 | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 | See the License for the specific language governing permissions and
 | limitations under the License.
 */
package com.esri.geoevent.solutions.adapter.eventJson;

import java.nio.BufferOverflowException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.OutboundAdapterBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldGroup;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;

public class EventJsonOutboundAdapter extends OutboundAdapterBase {
	private static final Log LOG = LogFactory
			.getLog(EventJsonOutboundAdapter.class);
	private StringBuilder stringBuilder = new StringBuilder();
	private Charset charset;

	public EventJsonOutboundAdapter(AdapterDefinition definition)
			throws ComponentException {
		super(definition);
		charset = StandardCharsets.UTF_8;
	}

	@Override
	public void receive(GeoEvent geoEvent) {
		try {
			String eventJson = constructEventString(geoEvent);
			stringBuilder.append(eventJson);
			super.receive(charset.encode(eventJson), geoEvent.getTrackId(), geoEvent);

		} catch (BufferOverflowException ex) {
			LOG.error("Csv Outbound Adapter does not have enough room in the buffer to hold the outgoing data.  Either the receiving transport object is too slow to process the data, or the data message is too big.");
		}
		catch(Exception e)
		{
			LOG.error(e.getMessage());
		}
		
	}

	private String constructEventString(GeoEvent event) {

		List<FieldDefinition> fds = event.getGeoEventDefinition()
				.getFieldDefinitions();
		Boolean first = true;
		String eventString = "{";
		eventString += addStringQuotes("event") + ": {";
		eventString += addStringQuotes("defname") + ":";
		eventString += addStringQuotes(event.getGeoEventDefinition().getName()) + ",";
		eventString += addStringQuotes("defguid") + ":";
		eventString += addStringQuotes(event.getGeoEventDefinition().getGuid()) + ",";
		eventString += addStringQuotes("owner") + ":";
		eventString += addStringQuotes(event.getGeoEventDefinition().getOwner()) + ",";
		eventString += addStringQuotes("uri") + ":";
		eventString += addStringQuotes(definition.getUri().toString()) + ",";
		eventString += addStringQuotes("attributes") + ": {";
		for (FieldDefinition fd : fds) {
			try {
				if(!first)
					eventString += ", ";
				else
					first=false;
				String fs = generateFieldString(fd, event);
				eventString += fs;
			} catch (Exception e) {
				LOG.error(e.getMessage());
			}
		}
		eventString += "}}}";
		return eventString;
	}

	private String generateTagsString(List<String> tags) {
		Boolean first = true;
		String tagsString = addStringQuotes("tags") + ":[";
		for (String tag : tags) {
			if (!first) 
				tagsString += ",";
			else
				first = false;
			tagsString += addStringQuotes(tag);
		}
		tagsString += "]";
		return tagsString;
	}

	private String addStringQuotes(String s) {
		String quotes = "\"";
		return quotes + s + quotes;
	}
	
	private String generateFieldString(FieldDefinition fd, FieldGroup event) throws Exception
	{
		FieldType type = fd.getType();
		String fieldString = "";
		String name = fd.getName();
		fieldString += addStringQuotes(name) + ":{";
		fieldString += addStringQuotes("type") + ":";
		fieldString += addStringQuotes(GeoEventTupleHelper
				.GetFieldTypeString(type)) + ",";
		List<String> tags = fd.getTags();
		if (!tags.isEmpty()) {
			fieldString += generateTagsString(tags) + ",";
		}
		fieldString += addStringQuotes("value") + ":";

		fieldString += convertValueToString(fd, event.getField(name));
		fieldString += "}";
		return fieldString;
	}
	
	private String convertValueToString(FieldDefinition fd, Object value)
			throws Exception {
		String v = null;
		FieldType t = fd.getType();
		try {

			if (t == FieldType.Geometry) {
				MapGeometry mapGeo = (MapGeometry) value;
				Geometry geo = mapGeo.getGeometry();
				SpatialReference sr = mapGeo.getSpatialReference();
				v = GeometryEngine.geometryToJson(sr, geo);
			}
			else if (t==FieldType.String)
			{
				v = addStringQuotes(value.toString());
			}
			else if (t==FieldType.Date)
			{
				v = addStringQuotes(value.toString());
			}
			else if (t == FieldType.Group) {
				List<FieldDefinition> children = fd.getChildren();
				FieldGroup fg = (FieldGroup) value;
				for (FieldDefinition child : children) {
					v = generateFieldString(child, fg);
				}
			} else {
				v = value.toString();
			}

		} catch (Exception e) {
			LOG.error(e.getMessage());
			throw (e);
		}
		return v;
	}
		
	
}