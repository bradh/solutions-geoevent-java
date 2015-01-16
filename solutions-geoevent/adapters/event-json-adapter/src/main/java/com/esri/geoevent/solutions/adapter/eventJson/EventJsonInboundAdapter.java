package com.esri.geoevent.solutions.adapter.eventJson;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.james.mime4j.field.datetime.DateTime;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.DefaultGeoEventDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.MessagingException;

public class EventJsonInboundAdapter extends InboundAdapterBase {
	private static final Log LOG = LogFactory.getLog(EventJsonInboundAdapter.class);
	private byte[] bytes = null;
	public EventJsonInboundAdapter(AdapterDefinition definition)
			throws ComponentException {
		super(definition);
	}

	@Override
	public void receive(ByteBuffer buffer, String channelId) {
		int remaining = buffer.remaining();
		if (remaining <= 0)
			return;
		if (bytes == null) {
			bytes = new byte[remaining];
			buffer.get(bytes);
		} else {
			byte[] temp = new byte[bytes.length + remaining];
			System.arraycopy(bytes, 0, temp, 0, bytes.length);
			buffer.get(temp, bytes.length, remaining);
			bytes = temp;
		}
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			BufferedReader br = new BufferedReader(new InputStreamReader(bais));
			char[] streamBuff = new char[2048];
			StringBuilder receivedData = new StringBuilder();
			int data = 0;

			while ((data = br.read(streamBuff)) > 0) {
				receivedData.append(streamBuff, 0, data);
			}
			String json = receivedData.toString();
			GeoEvent event = generateEventFromJson(json);
			geoEventListener.receive(event);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

	}
	@Override
	protected GeoEvent adapt(ByteBuffer arg0, String arg1) {
		return null;
	}
	
	@SuppressWarnings({ "unused", "unchecked" })
	private GeoEvent generateEventFromJson(String eventJson) throws GeoEventDefinitionManagerException, MessagingException, ConfigurationException, IOException, FieldException
	{
		GeoEvent event = null;
		try {
			//Map<String, Object> map = new HashMap<String, Object>();
			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> map = mapper.readValue(eventJson,  new TypeReference<HashMap<String, Object>>(){});
			GeoEventDefinition ged = null;
			String defname = properties.get("outputDef").getValueAsString();
			GeoEventDefinitionManager manager = geoEventCreator.getGeoEventDefinitionManager();
			HashMap<String, Object> valMap = new HashMap<String, Object>();
			HashMap<String, Object> attributes = (HashMap<String, Object>)map.get("attributes");
			Set<String>keys = attributes.keySet();
			if((ged=manager.searchGeoEventDefinition(defname, getId()))==null)
			{
				List<FieldDefinition> fieldDefs = new ArrayList<FieldDefinition>();
				for(String k:keys)
				{
					@SuppressWarnings("unchecked")
					HashMap<String, Object>att = (HashMap<String, Object>)attributes.get(k);
					FieldType type = GeoEventTupleHelper.GetFieldTypeFromString((String)att.get("type"));
					List<String> tagList = null;
					if(att.containsKey("tags"))
					{
						tagList = (List<String>)att.get("tags");
					}
					String[] tags = null;
					
					if(tagList != null)
					{
						tags = (String[])tagList.toArray();
					}
					FieldDefinition fd = new DefaultFieldDefinition(k, type, tags);
					Object val = parseValue(type, att.get("value"));
					valMap.put(k, val);
				}
			
				
				ged = new DefaultGeoEventDefinition();
				ged.setFieldDefinitions(fieldDefs);
				ged.setOwner(getId());
				ged.setName(defname);
				manager.addGeoEventDefinition(ged);
			}
			else
			{
				for(String k:keys)
				{
					HashMap<String, Object>att = (HashMap<String, Object>)attributes.get(k);
					FieldType type = GeoEventTupleHelper.GetFieldTypeFromString((String)att.get("type"));
					Object val = parseValue(type, att.get("value"));
					valMap.put(k, val);
				}
			}
			event = geoEventCreator.create(ged.getGuid());
			List<FieldDefinition> fds = ged.getFieldDefinitions();
			for(FieldDefinition f: fds)
			{
				event.setField(f.getName(), valMap.get(f.getName()));
			}
			
		} catch (MessagingException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			throw(e);
		} catch (JsonParseException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			throw(e);
		} catch (JsonMappingException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			throw(e);
		} catch (IOException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			throw(e);
		} catch (ConfigurationException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			throw(e);
		} catch (FieldException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace());
			throw(e);
		}
		
		return event;
	}
	
	private Object parseValue(FieldType t, Object value) throws IOException
	{
		Object v = null;
		if(t==FieldType.Boolean)
		{
			v = (Boolean)value;
		}
		else if(t==FieldType.Date)
		{
			String d = (String) value;
			String format = properties.get("dateFormat").getValueAsString();
			DateFormat formatter = new SimpleDateFormat(format);
			try {
				v = formatter.parse(d);
			} catch (ParseException e) {
				LOG.error(e.getMessage());
				return null;
			}
			
		}
		else if(t==FieldType.Double)
		{
			v = (Double)value;
		}
		else if(t==FieldType.Float)
		{
			v = (Float)value;
		}
		else if(t==FieldType.Geometry)
		{
			String json = (String)value;
			MapGeometry mapGeo = GeometryEngine.jsonToGeometry(json);
			v = mapGeo;
			
		}
		else if(t==FieldType.Group)
		{
			//type="group";
		}
		else if(t==FieldType.Integer)
		{
			v = (Integer)value;
		}
		else if(t==FieldType.Long)
		{
			v = (Long)value;
		}
		else if(t==FieldType.Short)
		{
			v = (Short)value;
		}
		else if(t==FieldType.String)
		{
			v = (String)value;
		}
		else
		{
			IOException e = new IOException("Invalid input");
			LOG.error(e.getMessage());
			throw(e);
		}
		return v;
	}
}
