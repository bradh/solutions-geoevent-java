package com.esri.geoevent.solutions.processor.hotspot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import backtype.storm.tuple.Tuple;

public class GenerateHotspotEventBolt extends BaseRichBolt {
	private static final Log LOG = LogFactory
			.getLog(GenerateHotspotEventBolt.class);
	public GeoEventDefinitionManager manager;
	public Messaging messaging;
	private String gedNamePrefix;
	private HotspotProcessor processor;
	GenerateHotspotEventBolt(String gedNamePrefix){
		this.gedNamePrefix=gedNamePrefix;
	}
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		String cellId = tuple.getStringByField("cellId");
		String owner = tuple.getStringByField("owner");
		Long count = tuple.getLongByField("count");
		String cellGeo = tuple.getStringByField("cellGeometry");
		try {
			GeoEventDefinition ged = FindGeoEventDefinition(owner, procId);
			GeoEvent event = createEvent(ged, procId, cellId, count, cellGeo);
			processor.queue.add(event);
		} catch (GeoEventDefinitionManagerException e) {
			LOG.error(e.getMessage());
		} catch (JsonParseException e) {
			LOG.error(e.getMessage());
		} catch (MessagingException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} catch (FieldException e) {
			LOG.error(e.getMessage());
		}

	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		;
	}
	
	private GeoEvent createEvent(GeoEventDefinition ged, String procId,
			String cellId, Long count, String geo) throws MessagingException,
			JsonParseException, IOException, FieldException {
		try {
			GeoEventCreator creator = this.messaging.createGeoEventCreator();
			GeoEvent event = creator.create(ged.getGuid());
			MapGeometry mapGeo = GeometryEngine.jsonToGeometry(geo);

			event.setField("CellId", cellId);
			event.setField("ProcId", procId);
			event.setField("count", count);
			event.setGeometry(mapGeo);
			return event;
		} catch (FieldException e) {
			LOG.error(e.getMessage());
			throw (e);
		} catch (JsonParseException e) {
			LOG.error(e.getMessage());
			throw (e);
		} catch (MessagingException e) {
			LOG.error(e.getMessage());
			throw (e);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			throw (e);
		}
	}

	private GeoEventDefinition FindGeoEventDefinition(String owner,
			String procId) throws GeoEventDefinitionManagerException {
		GeoEventDefinition ged = null;
		String gedname = gedNamePrefix + "_" + procId;
		if ((ged = manager.searchGeoEventDefinition(gedname, owner)) == null) {
			ged = new DefaultGeoEventDefinition();
			List<FieldDefinition> fldDefs = new ArrayList<FieldDefinition>();
			FieldDefinition idFd;
			try {
				idFd = new DefaultFieldDefinition("CellId", FieldType.String,
						"TRACK_ID");
				fldDefs.add(idFd);
				FieldDefinition procFd = new DefaultFieldDefinition("ProcId",
						FieldType.String, "PROCESS_ID");
				fldDefs.add(procFd);
				FieldDefinition countFd = new DefaultFieldDefinition("count",
						FieldType.Long);
				fldDefs.add(countFd);
				FieldDefinition geoFd = new DefaultFieldDefinition("geometry",
						FieldType.Geometry, "GEOMETRY");
				fldDefs.add(geoFd);
				ged.setFieldDefinitions(fldDefs);
				ged.setOwner(owner);
				ged.setName(gedname);
				manager.addGeoEventDefinition(ged);
			} catch (ConfigurationException e) {
				LOG.error(e.getMessage());
			}
		}

		return ged;
	}
	
	// getters setters
	public void setMessaging(Messaging messaging) {
		this.messaging = messaging;
	}

	public void setManager(GeoEventDefinitionManager manager) {
		this.manager = manager;
	}
	
	public void setProcessor(HotspotProcessor processor)
	{
		this.processor = processor;
	}

}
