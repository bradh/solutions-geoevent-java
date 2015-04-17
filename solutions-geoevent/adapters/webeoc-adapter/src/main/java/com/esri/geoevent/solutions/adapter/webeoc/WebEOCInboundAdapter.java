package com.esri.geoevent.solutions.adapter.webeoc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
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
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.util.DateUtil;

public class WebEOCInboundAdapter extends InboundAdapterBase {
	private String eventDef;
	private Boolean useGeocoding;
	private Boolean createdef = false;
	private String geocodeService = null;
	private String xField;
	private String yField;

	private String arguments;
	private Integer score;
	private SAXParserFactory saxFactory;
	private WebEOCMessageParser messageParser;
	private SAXParser saxParser;
	private DefaultHandler handler;
	private static final Log LOG = LogFactory
			.getLog(WebEOCInboundAdapter.class);
	private GeoEventDefinitionManager manager;
	private GeoEventDefinition ged;
	private Boolean hasGeometry;
	private final ArrayList<GeoEvent> queue = new ArrayList<GeoEvent>();
	private final boolean tryingToRecoverPartialMessages = false;
	private byte[] bytes = null;
	private final static String dblRegex = "[\\+\\-]?\\d+\\.\\d+(?:[eE][\\+\\-]?\\d+)?";
	private final static String intRegex = "[\\+\\-]?\\d+";
	private static final String boolRegex = "^(True|False|TRUE|FALSE|true|false|Yes|No|YES|NO|yes|no|y|n)$";
	private static final String tokenRegex = "\\$\\{(.*?)\\}$";
	private Pattern DOUBLE_PATTERN;
	private Pattern INTEGER_PATTERN;
	private Pattern BOOLEAN_PATTERN;
	private Pattern TOKEN_PATTERN;
	private String pattern;

	public WebEOCInboundAdapter(AdapterDefinition definition)
			throws ComponentException {
		super(definition);
		try {

			DOUBLE_PATTERN = Pattern.compile(dblRegex);
			INTEGER_PATTERN = Pattern.compile(intRegex);
			BOOLEAN_PATTERN = Pattern.compile(boolRegex);
			TOKEN_PATTERN = Pattern.compile(tokenRegex);
			messageParser = new WebEOCMessageParser(this);
			saxFactory = SAXParserFactory.newInstance();
			saxParser = saxFactory.newSAXParser();
		} catch (ParserConfigurationException e) {
			LOG.error(e.getMessage());
		} catch (SAXException e) {
			LOG.error(e.getMessage());
		}
		catch(PatternSyntaxException e)
		{
			LOG.error(e.getMessage());
		}
	}

	@Override
	public void afterPropertiesSet() {
		try {
			eventDef = properties.get("geoeventDefinition").getValueAsString();
			hasGeometry = (Boolean) properties.get("hasGeometry").getValue();
			useGeocoding = (Boolean) properties.get("useGeocoding").getValue();
			if (hasGeometry) {
				xField = properties.get("x").getValueAsString();
				yField = properties.get("y").getValueAsString();
			}
			if (useGeocoding) {
				geocodeService = properties.get("geocodeService")
						.getValueAsString();
				score = (Integer)properties.get("score")
						.getValue();
				arguments = properties.get("geocodeargs").getValueAsString();
				/*String tmpCity = properties.get("cityfield")
						.getValueAsString();
				if(!tmpCity.isEmpty())
				{
					cityField=tmpCity;
				}
				String tmpState=properties.get("statefield").getValueAsString();
				if(!tmpState.isEmpty())
				{
					statefield=tmpState;
				}
				String tmpCountry=properties.get("countryfield").getValueAsString();	
				if(!tmpCountry.isEmpty())
				{
					countryfield=tmpState;
				}
				String tmpZip = properties.get("zipfield")
						.getValueAsString();
				if(!tmpZip.isEmpty())
				{
					zipField = tmpZip;
				}*/
				
			}
			manager = geoEventCreator.getGeoEventDefinitionManager();
			Collection<GeoEventDefinition> gedColl = manager
					.searchGeoEventDefinitionByName(eventDef);
			Iterator<GeoEventDefinition> it = gedColl.iterator();
			if (gedColl.isEmpty()) {
				createdef = true;

			} else {
				ged = it.next();

			}
		} catch (Exception e) {

		}
	}

	@Override
	public synchronized void validate() throws ValidationException {

		if (useGeocoding) {
			if (geocodeService == null || arguments == null) {
				ValidationException e = new ValidationException(
						"Geocode Service and Geocode Field must not be empty.");
				throw (e);
			}
		}
		if (!createdef) {
			List<String> tags = ged.getTagNames();
			if (!tags.contains("TRACK_ID")) {
				ValidationException e = new ValidationException(
						"The GeoEvent Definition must have a valid TRACK_ID.");
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
		try {

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
				saxParser.parse(new ByteArrayInputStream(bytes), messageParser);
				bytes = null;
				commit();
			} catch (SAXException e) {
				LOG.error(
						"SAXException while trying to parse the incoming xml.",
						e);

				if (tryingToRecoverPartialMessages) {
					queue.clear();
				} else {
					bytes = null;
					commit();
				}
			}
		} catch (IOException e) {
			LOG.error(
					"IOException while trying to route data from the byte buffer to the pipe.",
					e);
		}
	}

	public Boolean getCreateDef() {
		return createdef;
	}

	public void setCreateDef(Boolean create) {
		this.createdef = create;
	}

	public void setDatePattern(String pattern) {
		this.pattern = pattern;
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

						SimpleDateFormat sdf = new SimpleDateFormat(pattern);
						sdf.setLenient(false);
						Date date = sdf.parse(fieldValue);
						geoEvent.setField(fieldName, date);
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
			if (hasGeometry) {
				Double x = Double.parseDouble(fields.get(xField));
				Double y = Double.parseDouble(fields.get(yField));
				Point p = new Point(x, y);
				SpatialReference sr = SpatialReference.create(4326);
				MapGeometry mapGeo = new MapGeometry(p, sr);
				try {
					geoEvent.setGeometry(mapGeo);
				} catch (FieldException e) {
					LOG.error(e.getMessage());
				}
			}
			else
			{
				if(useGeocoding)
				{
					MapGeometry geo = geocode(geocodeService, arguments, fields);
					SpatialReference sr = SpatialReference.create(4326);
					try {
						geoEvent.setGeometry(geo);
					} catch (FieldException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			}
		}
		queue.add(geoEvent);

	}

	public void CreateDef(HashMap<String, String> attributes) {

		try {
			Set<String> keys = attributes.keySet();
			Iterator<String> it = keys.iterator();
			List<FieldDefinition> fldDefs = new ArrayList<FieldDefinition>();
			FieldDefinition fd = new DefaultFieldDefinition("trackid",
					FieldType.String, "TRACK_ID");
			if (hasGeometry || useGeocoding) {
				fd = new DefaultFieldDefinition("geometry", FieldType.Geometry,
						"GEOMETRY");
			}
			fldDefs.add(fd);
			while (it.hasNext()) {
				String key = it.next();
				FieldType type = interpolateType(attributes.get(key));
				fd = new DefaultFieldDefinition(key, type);
				fldDefs.add(fd);
			}
			ged = new DefaultGeoEventDefinition();
			ged.setFieldDefinitions(fldDefs);
			ged.setName(eventDef);
			ged.setOwner(definition.getUri().toString());
			manager.addGeoEventDefinition(ged);
		} catch (ConfigurationException e) {
			LOG.error("Error configuring field");
			LOG.error(e.getMessage());
		} catch (GeoEventDefinitionManagerException e) {
			LOG.error("Error adding GeoEvent Definition");
			LOG.error(e.getMessage());
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

	private Boolean isValidDate(String in, String pattern) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			sdf.setLenient(false);
			sdf.parse(in);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	private FieldType interpolateType(String in) {

		List<String> dateFormats = new ArrayList<String>();
		dateFormats.add("yyyy-MM-dd");
		dateFormats.add("yyyy/MM/dd");

		dateFormats.add("yyyy/MM/ddThh:mm:ss a");
		dateFormats.add("yyyy-MM-ddThh:mm:ss a");
		dateFormats.add("yyyy/MM/dd hh:mm:ss a");
		dateFormats.add("yyyy-MM-dd hh:mm:ss a");
		dateFormats.add("yyyy/MM/ddTHH:mm:ss");
		dateFormats.add("yyyy-MM-ddTHH:mm:ss");
		dateFormats.add("yyyy/MM/dd HH:mm:ss");
		dateFormats.add("yyyy-MM-dd HH:mm:ss");

		dateFormats.add("yyyy/MM/ddThh:mm a");
		dateFormats.add("yyyy-MM-ddThh:mm a");
		dateFormats.add("yyyy/MM/dd hh:mm a");
		dateFormats.add("yyyy-MM-dd hh:mm a");
		dateFormats.add("yyyy/MM/ddTHH:mm");
		dateFormats.add("yyyy-MM-ddTHH:mm");
		dateFormats.add("yyyy/MM/dd HH:mm");
		dateFormats.add("yyyy-MM-dd HH:mm");

		dateFormats.add("MM-dd-yyyy");
		dateFormats.add("MM/dd/yyyy");

		dateFormats.add("MM/dd/yyyyThh:mm:ss a");
		dateFormats.add("MM-dd-yyyyThh:mm:ss a");
		dateFormats.add("MM/dd/yyyy hh:mm:ss a");
		dateFormats.add("MM-dd-yyyy hh:mm:ss a");
		dateFormats.add("MM/dd/yyyyTHH:mm:ss");
		dateFormats.add("MM-dd-yyyyTHH:mm:ss");
		dateFormats.add("MM/dd/yyyy HH:mm:ss");
		dateFormats.add("MM-dd-yyyy HH:mm:ss");

		dateFormats.add("MM/dd/yyyyThh:mm a");
		dateFormats.add("MM-dd-yyyyThh:mm a");
		dateFormats.add("MM/dd/yyyy hh:mm a");
		dateFormats.add("MM-dd-yyyy hh:mm a");
		dateFormats.add("MM/dd/yyyyTHH:mm");
		dateFormats.add("MM-dd-yyyyTHH:mm");
		dateFormats.add("MM/dd/yyyy HH:mm");
		dateFormats.add("MM-dd-yyyy HH:mm");

		for (String pattern : dateFormats) {
			if (isValidDate(in, pattern)) {
				setDatePattern(pattern);
				return FieldType.Date;
			}
		}
		if (DOUBLE_PATTERN.matcher(in).matches()) {
			return FieldType.Double;
		} else if (INTEGER_PATTERN.matcher(in).matches()) {
			return FieldType.Integer;
		} else if (BOOLEAN_PATTERN.matcher(in).matches()) {
			return FieldType.Boolean;
		}
		return FieldType.String;
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

	private void commit() {
		for (GeoEvent geoEvent : queue) {
			if (geoEvent != null)
				geoEventListener.receive(geoEvent);
		}
		queue.clear();
	}

	private MapGeometry geocode(String geocodeService,
			String arguments, HashMap<String, String> fields) {
		MapGeometry mapgeo = null;
		String contentType = "application/json";
		HttpClient httpclient = HttpClientBuilder.create().build();
		try {
			/*String args = "Address=";
			String address = URLEncoder.encode(geocodeString, "UTF-8");
			args += address;
			if (city != null) {
				city = URLEncoder.encode(city, "UTF-8");
				args += "&City=" + city;
			}
			if (zip != null) {
				zip = URLEncoder.encode(zip, "UTF-8");
				args += "&Zip=" + zip;
			}*/
			String args = parseArguments(arguments, fields);
			String uri = geocodeService + "/" + "findAddressCandidates?" + args;
			try {
				HttpPost httppost = new HttpPost(uri);
				httppost.setHeader("Accept", contentType);
				HttpResponse response = httpclient.execute(httppost);

				HttpEntity entity = response.getEntity();
				if (entity != null) {
					InputStream instream = entity.getContent();
					try {
						BufferedReader br = new BufferedReader(
								new InputStreamReader((instream)));
						String output = "";
						String ln;
						while ((ln = br.readLine()) != null) {
							output += ln;
						}
						Map<String, Object> map = new HashMap<String, Object>();
						ObjectMapper mapper = new ObjectMapper();
						map = mapper.readValue(output,
								new TypeReference<HashMap<String, Object>>() {
								});
						HashMap<String, Object> spRef = (HashMap<String, Object>)map.get("spatialReference");
						Integer coordWkid = (Integer)spRef.get("wkid");
						List<Object>candidates = (List<Object>) map.get("candidates");
						HashMap<String, Object> item = (HashMap<String, Object>)candidates.get(0);
						Integer geocodescore = (Integer)item.get("score");
						if(score <= geocodescore )
						{
							HashMap<String, Double> location = (HashMap<String, Double>)item.get("location");
							Double x = location.get("x");
							Double y = location.get("y");
							Geometry geo = new Point(x, y);
							SpatialReference sr = SpatialReference.create(coordWkid);
							mapgeo = new MapGeometry(geo, sr);
							
						}
						
					} catch (IOException ex) {
						// In case of an IOException the connection will be
						// released
						// back to the connection manager automatically
						LOG.error(ex);
						throw ex;
					} catch (RuntimeException ex) {
						// In case of an unexpected exception you may want to
						// abort
						// the HTTP request in order to shut down the underlying
						// connection immediately.
						LOG.error(ex);
						httppost.abort();
						throw ex;
					} catch (Exception ex) {

						LOG.error(ex);
						httppost.abort();
						throw ex;
					} finally {
						// Closing the input stream will trigger connection
						// release
						try {
							instream.close();
						} catch (Exception ignore) {
						}
					}
				}

			} catch (Exception ex) {
				LOG.error(ex);
				ex.printStackTrace();
			}

		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
		return mapgeo;
	}
	
	private String parseArguments(String arguments,
			HashMap<String, String> fields)
			throws UnsupportedEncodingException, Exception {
		try {
			List<String> arglist = Arrays.asList(arguments.split(","));

			String args = "";
			Boolean isFirstIt = true;
			for (String arg : arglist) {
				if (!isFirstIt) {
					args += "&";
				}
				isFirstIt = false;
				String[] splitArg = arg.split(":");
				String argname = splitArg[0];
				String tmpArgVal = splitArg[1];
				String val = null;
				if (TOKEN_PATTERN.matcher(tmpArgVal).matches()) {
					String fieldname = tmpArgVal.substring(2, (tmpArgVal.length() - 1));
					val = fields.get(fieldname);
				} else {
					val = tmpArgVal;
				}
				
				val = URLEncoder.encode(val, "UTF-8");
				String argument = argname + "=" + val;
				args += argument;
			}
			args += "&f=json";
			return args;
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			LOG.error(e.getMessage());
			throw e;
		}
	}
	
	private String replaceIllegalChar(String in)
	{
		
		in = in.replace("<", "&lt");
		in = in.replace("&", "&amp");
		in = in.replace(">", "&gt");
		in = in.replace("\"", "&quot");
		in = in.replace("'", "&apos");
		return in;
	}

}
