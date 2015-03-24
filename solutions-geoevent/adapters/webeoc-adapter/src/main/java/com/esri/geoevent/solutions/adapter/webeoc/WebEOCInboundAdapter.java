package com.esri.geoevent.solutions.adapter.webeoc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.ges.adapter.AdapterDefinition;
import com.esri.ges.adapter.InboundAdapterBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.util.DateUtil;

public class WebEOCInboundAdapter extends InboundAdapterBase {
	private String eventDef;
	private Boolean useGeocoding;
	private String geocodeService = null;
	private String geocodeField = null;
	private SAXParserFactory saxFactory;
	private WebEOCMessageParser messageParser;
	private SAXParser saxParser;
	private DefaultHandler handler;
	private static final Log LOG = LogFactory
			.getLog(WebEOCInboundAdapter.class);
	private GeoEventDefinitionManager manager;
	private GeoEventDefinition ged;
	private final ArrayList<GeoEvent> queue = new ArrayList<GeoEvent>();
	private final boolean tryingToRecoverPartialMessages = false;
	private byte[] bytes = null;
	
	public WebEOCInboundAdapter(AdapterDefinition definition)
			throws ComponentException {
		super(definition);
		try {

			messageParser = new WebEOCMessageParser(this);
			saxFactory = SAXParserFactory.newInstance();
			saxParser = saxFactory.newSAXParser();
		} catch (ParserConfigurationException e) {
			LOG.error(e.getMessage());
		} catch (SAXException e) {
			LOG.error(e.getMessage());
		}
	}

	@Override
	public void afterPropertiesSet() {
		try {
			eventDef = properties.get("geoeventDefinition").getValueAsString();
			useGeocoding = (Boolean) properties.get("useGeocoding").getValue();
			if (!properties.get("").getValueAsString().isEmpty()) {
				geocodeService = properties.get("geocodeService")
						.getValueAsString();
			}
			if (!properties.get("geoCodeField").getValueAsString().isEmpty()) {
				geocodeField = properties.get("geocodeField")
						.getValueAsString();
			}
		} catch (Exception e) {

		}
	}

	@Override
	public synchronized void validate() throws ValidationException {
		manager = geoEventCreator.getGeoEventDefinitionManager();
		Collection<GeoEventDefinition> gedColl = manager
				.searchGeoEventDefinitionByName(eventDef);
		Iterator<GeoEventDefinition> it = gedColl.iterator();
		if (gedColl.isEmpty()) {
			ValidationException e = new ValidationException(
					"Cannot locate Geoevent definition");
			throw (e);
		} else {
			ged = it.next();
		}
		if (useGeocoding) {
			if (geocodeService == null || geocodeField == null) {
				ValidationException e = new ValidationException(
						"Geocode Service and Geocode Field must not be empty.");
				throw (e);
			}
		}
	}

	@Override
	protected GeoEvent adapt(ByteBuffer buffer, String channelId) {
		// Don't need to implement this class because we are overriding the base
		// class's implementation of the receive() function, which prevents this
		// method from being called.
		return null;
	}

	@Override
	public void receive(ByteBuffer buffer, String channelId) {
		try
		{
		int remaining = buffer.remaining();
		if (remaining <= 0)
			return;
		if (bytes == null)
		{
			bytes = new byte[remaining];
			buffer.get(bytes);
		}
		else
		{
			byte[] temp = new byte[bytes.length + remaining];
			System.arraycopy(bytes, 0, temp, 0, bytes.length);
			buffer.get(temp, bytes.length, remaining);
			bytes = temp;
		}
		try
		{
			saxParser.parse(new ByteArrayInputStream(bytes), messageParser);
			bytes = null;
			commit();
		}
		catch (SAXException e)
		{
			LOG.error("SAXException while trying to parse the incoming xml.", e);

			// TODO : figure out a way to recover the lost bytes. for now, just
			// throwing them away.
			if (tryingToRecoverPartialMessages)
			{
				queue.clear();
			}
			else
			{
				bytes = null;
				commit();
			}
		}
	}
	catch (IOException e)
	{
		LOG.error("IOException while trying to route data from the byte buffer to the pipe.", e);
	}
	}
	

	@SuppressWarnings("incomplete-switch")
	public void queueGeoEvent(HashMap<String, String> fields) {
		// in.mark(4 * 1024);

		GeoEvent geoEvent = findAndCreate(eventDef);
		if (geoEvent == null) {
			LOG.error("The incoming GeoEvent of type \""
					+ eventDef
					+ "\" does not have a corresponding Event Definition in the ArcGIS GeoEvent server.");
		} else {
			GeoEventDefinition definition = geoEvent.getGeoEventDefinition();
			for (String fieldName : fields.keySet()) {
				String fieldValue = fields.get(fieldName);
				try {
					FieldDefinition fieldDefinition = definition
							.getFieldDefinition(fieldName);
					if (fieldDefinition == null) {
						LOG.error("The incoming GeoEvent of type \""
								+ eventDef
								+ "\" had an attribute called \""
								+ fieldName
								+ "\"that does not exist in the corresponding Event Definition.");
						continue;
					}
					switch (fieldDefinition.getType()) {
					case Integer:
						geoEvent.setField(fieldName,
								Integer.parseInt(fieldValue));
						break;
					case Long:
						geoEvent.setField(fieldName, Long.parseLong(fieldValue));
						break;
					case Short:
						geoEvent.setField(fieldName,
								Short.parseShort(fieldValue));
						break;
					case Double:
						geoEvent.setField(fieldName,
								Double.parseDouble(fieldValue));
						break;
					case Float:
						geoEvent.setField(fieldName,
								Float.parseFloat(fieldValue));
						break;
					case Boolean:
						geoEvent.setField(fieldName,
								Boolean.parseBoolean(fieldValue));
						break;
					case Date:
						geoEvent.setField(fieldName,
								DateUtil.convert(fieldValue));
						break;
					case String:
						geoEvent.setField(fieldName, fieldValue);
						break;
					case Geometry:
						String geometryString = fieldValue;
						if (geometryString.contains(";"))
							geometryString = geometryString.substring(0,
									geometryString.indexOf(';') - 1);
						String[] g = geometryString.split(",");
						double x = Double.parseDouble(g[0]);
						double y = Double.parseDouble(g[1]);
						double z = 0;
						if (g.length > 2)
							z = Double.parseDouble(g[2]);
						int wkid = Integer.parseInt(fields.get("_wkid"));
						// Point point = spatial.createPoint(x, y, z, wkid);
						Point point = new Point(x, y, z);
						SpatialReference sref = SpatialReference.create(wkid);
						MapGeometry mapGeo = new MapGeometry(point, sref);
						// int geometryID =
						// geoEvent.getGeoEventDefinition().getGeometryId();
						geoEvent.setGeometry(mapGeo);
						break;
					}
				} catch (Exception ex) {
					LOG.warn("Error wile trying to parse the GeoEvent field "
							+ fieldName + ":" + fieldValue, ex);
				}
			}
		}
		queue.add(geoEvent);

	}

	private GeoEvent findAndCreate(String name) {
		Collection<GeoEventDefinition> results = geoEventCreator
				.getGeoEventDefinitionManager().searchGeoEventDefinitionByName(
						name);
		if (!results.isEmpty()) {
			try {
				return geoEventCreator.create(results.iterator().next()
						.getGuid());
			} catch (MessagingException e) {
				LOG.error("GeoEvent creation failed: " + e.getMessage());
			}
		} else
			LOG.error("GeoEvent creation failed: GeoEvent definition '" + name
					+ "' not found.");
		return null;
	}
	
	private void commit()
	{
		for (GeoEvent geoEvent : queue)
		{
			if( geoEvent != null )
				geoEventListener.receive(geoEvent);
		}
		queue.clear();
	}

}
