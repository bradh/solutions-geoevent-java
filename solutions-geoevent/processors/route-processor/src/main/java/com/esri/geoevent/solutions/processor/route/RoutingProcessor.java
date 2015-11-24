package com.esri.geoevent.solutions.processor.route;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.DefaultGeoEventDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.datastore.agsconnection.ArcGISServerConnection;
import com.esri.ges.manager.datastore.agsconnection.ArcGISServerConnectionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class RoutingProcessor extends GeoEventProcessorBase implements
GeoEventProducer, EventUpdatable{
	private ArcGISServerConnectionManager connectionManager;
	private Messaging messaging;
	private GeoEventDefinitionManager manager;
	private GeoEventProducer geoEventProducer;
	private GeoEventDefinition edOut;
	private GeoEventDefinition dirEdOut;
	private String connName;
	private String naLayer;
	private String baseUrl;
	private String stopsLayer;
	private String stopsJson;
	private String ptBarriers;
	private String ptBarrierJson;
	private String lineBarriers;
	private String lineBarrierJson;
	private String polygonBarriers;
	private String polygonBarrierJson;
	private boolean hasPtBarriers;
	private boolean hasLineBarriers;
	private boolean hasPolygonBarriers;
	private String token = null;
	private ArcGISServerConnection conn;
	private enum BarrierType {POINT, LINE, POLYGON}
	public RoutingProcessor(GeoEventProcessorDefinition definition)
			throws ComponentException {
		super(definition);
		// TODO Auto-generated constructor stub
	}

	public GeoEvent process(GeoEvent evt) throws Exception {
		GeoEvent geOut = null;
		String request = baseUrl + stopsJson;
		if(ptBarriers != null && !ptBarriers.isEmpty())
		{
			if(ptBarrierJson.equals("pointBarriers="))
			{
				ptBarrierJson = createBarrierJson(ptBarriers, BarrierType.POINT);
			}
			request += "&"+ptBarrierJson;
		}
		if(lineBarriers!= null && !lineBarriers.isEmpty())
		{
			if(lineBarrierJson.equals("polylineBarriers="))
			{
				lineBarrierJson = createBarrierJson(lineBarriers, BarrierType.LINE);
			}
			request += "&"+lineBarrierJson;
		}
		if(polygonBarriers!= null && !polygonBarriers.isEmpty())
		{
			if(polygonBarrierJson.equals("polygonBarriers="))
			{
				polygonBarrierJson = createBarrierJson(polygonBarriers, BarrierType.POLYGON);
			}
			request += "&"+polygonBarrierJson;
		}
		
		request += "&returnRoutes=true&f=json";
		if(token != null)
		{
			request += "&token=" + token;
		}
		try {
			String contentType = "application/json";
			HttpClient httpclient = HttpClientBuilder.create().build();
			HttpPost httpPost = new HttpPost(request);
			httpPost.setHeader("Accept", contentType);
			HttpResponse response = httpclient.execute(httpPost);
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
					@SuppressWarnings("unchecked")
					HashMap<String, Object> rmap = (HashMap<String, Object>) map.get("routes");
					
					HashMap<String, Object> srmap = (HashMap<String, Object>) rmap.get("spatialReference");
					Integer wkid = (Integer) srmap.get("wkid");
					@SuppressWarnings("unchecked")
					ArrayList<Object> features = (ArrayList<Object>) rmap.get("features");
					@SuppressWarnings("unchecked")
					Map<String, Object>f = (Map<String, Object>) features.get(0);
					@SuppressWarnings("unchecked")
					Map<String, Object> jsonGeom = (Map<String, Object>) f.get("geometry");
					@SuppressWarnings("unchecked")
					ArrayList<Object> paths = (ArrayList<Object>) jsonGeom.get("paths");
					@SuppressWarnings("unchecked")
					ArrayList<Object> path = (ArrayList<Object>) paths.get(0);
					Polyline route = new Polyline();
					for (int i = 0; i < path.size(); ++i)
					{
						@SuppressWarnings("unchecked")
						ArrayList<Object> coords = (ArrayList<Object>) path.get(i);
						Double x = (Double) coords.get(0);
						Double y = (Double)coords.get(1);
						Point p = new Point(x,y);
						if(i==0)
						{
							route.startPath(p);
						}
						else
						{
							route.lineTo(p);
						}
					}
					@SuppressWarnings("unchecked")
					HashMap<String, Object> attributes = (HashMap<String, Object>) f.get("attributes");
					GeoEventCreator geoEventCreator = messaging.createGeoEventCreator();
					geOut = geoEventCreator.create(edOut.getGuid());
					geOut.setProperty(GeoEventPropertyName.TYPE, "message");
					geOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
					geOut.setProperty(GeoEventPropertyName.OWNER_ID, definition.getUri());
					SpatialReference sr = SpatialReference.create(wkid);
					MapGeometry mapGeo = new MapGeometry(route, sr);
					geOut.setGeometry(mapGeo);
					geOut.setField("TRACK_ID", "1");
					geOut.setField("Name", attributes.get("Name"));
					geOut.setField("FirstStopID", attributes.get("FirstStopID"));
					geOut.setField("LastStopID", attributes.get("LastStopID"));
					geOut.setField("StopCount", attributes.get("StopCount"));
					geOut.setField("Total_Minutes", attributes.get("Total_Minutes"));
					@SuppressWarnings("unchecked")
					ArrayList<Object> directions= (ArrayList<Object>) map.get("directions");
					@SuppressWarnings("unchecked")
					HashMap<String, Object>direction = (HashMap<String, Object>) directions.get(0);
					@SuppressWarnings("unchecked")
					ArrayList<Object> dirFeatures = (ArrayList<Object>) direction.get("features");
					Integer count = 0;
					for(Object fobj: dirFeatures)
					{
						@SuppressWarnings("unchecked")
						HashMap<String, Object> dirFeat = (HashMap<String, Object>)fobj;
						String routeid = "1";
						String trackid = routeid + "_" + count.toString();
						++count;
						sendDirectionsGeoEvent(dirFeat, routeid, trackid);
					}
				} catch (IOException ex) {
					// In case of an IOException the connection will be
					// released
					// back to the connection manager automatically
					// LOG.error(ex);
					throw ex;
				} catch (RuntimeException ex) {
					// In case of an unexpected exception you may want to
					// abort
					// the HTTP request in order to shut down the underlying
					// connection immediately.
					// LOG.error(ex);
					httpPost.abort();
					throw ex;
				} catch (Exception ex) {

					// LOG.error(ex);
					httpPost.abort();
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

			ex.printStackTrace();
		}
		return geOut;
	}
	//getters setters
	public void setConnectionManager(ArcGISServerConnectionManager cm) {
		connectionManager = cm;
	}
	public void setMessaging(Messaging m)
	{
		messaging = m;
	}
	public void setManager(GeoEventDefinitionManager mgr)
	{
		manager = mgr;
	}
	
	@Override
	public void setId(String id) {
		super.setId(id);
		geoEventProducer = messaging
				.createGeoEventProducer(new EventDestination(id + ":event"));
	}
	
	@Override
	public boolean isGeoEventMutator()
	{
		return true;
	}
	@Override
	public void afterPropertiesSet()
	{
		
		naLayer = properties.get("naservice").getValueAsString();
		stopsLayer = properties.get("stops").getValueAsString();
		ptBarriers = properties.get("ptbarriers").getValueAsString();
		lineBarriers = properties.get("linebarriers").getValueAsString();
		polygonBarriers = properties.get("polygonbarriers").getValueAsString();
		baseUrl = createBaseUrl(naLayer);
		try {
			stopsJson = createStopsJson(stopsLayer);
		} catch (UnsupportedEncodingException e) {
			
			e.printStackTrace();
		}
		if(ptBarriers != null && !ptBarriers.isEmpty())
		{
			try {
				ptBarrierJson = createBarrierJson(ptBarriers, BarrierType.POINT);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(lineBarriers != null && !lineBarriers.isEmpty())
		{
			try {
				lineBarrierJson = createBarrierJson(lineBarriers, BarrierType.LINE);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(polygonBarriers != null && !polygonBarriers.isEmpty())
		{
			try {
				polygonBarrierJson = createBarrierJson(polygonBarriers, BarrierType.POLYGON);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		connName = properties.get("connection").getValueAsString();
		conn = connectionManager.getArcGISServerConnection(connName);
		token = conn.getDecryptedToken();
		if((manager.searchGeoEventDefinition("routing-processor", getId()))==null)
		{
			try {
				generateEventDef();
			} catch (ConfigurationException
					| GeoEventDefinitionManagerException e) {
				e.printStackTrace();
			}
		}
		else
		{
			edOut = manager.searchGeoEventDefinition("routing-processor", getId());
		}
		
		if((manager.searchGeoEventDefinition("routing-directions", getId()))==null)
		{
			try {
				generateDirectionsEventDef();
			} catch (ConfigurationException
					| GeoEventDefinitionManagerException e) {
				e.printStackTrace();
			}
		}
		else
		{
			dirEdOut = manager.searchGeoEventDefinition("routing-directions", getId());
		}
	}
	

	private void sendDirectionsGeoEvent(HashMap<String, Object> feature, String routeid, String trackid) throws FieldException
	{
		GeoEvent evt = null;
		try {
			evt = messaging.createGeoEventCreator().create(dirEdOut.getGuid());
			@SuppressWarnings("unchecked")
			HashMap<String, Object> attributes = (HashMap<String, Object>) feature.get("attributes");
			evt.setField("trackid", trackid);
			evt.setField("routeid", routeid);
			evt.setField("time", attributes.get("time"));
			evt.setField("text", attributes.get("text"));
			evt.setField("eta", attributes.get("ETA"));
			evt.setProperty(GeoEventPropertyName.TYPE, "event");
			evt.setProperty(GeoEventPropertyName.OWNER_ID, getId());
			evt.setProperty(GeoEventPropertyName.OWNER_URI,
					definition.getUri());

			send(evt);
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private boolean serviceHasFeatures(String url) throws ClientProtocolException, IOException
	{
		boolean hasFeatures = true;
		url = url + "/query?";
		String where = "where=";
		String clause = "1=1";
		clause = URLEncoder.encode(clause, "UTF-8");
		String request = url + where + clause + "&f=json";
		try {
			String contentType = "application/json";
			HttpClient httpclient = HttpClientBuilder.create().build();
			HttpPost httpPost = new HttpPost(request);
			httpPost.setHeader("Accept", contentType);
			HttpResponse response = httpclient.execute(httpPost);
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
					@SuppressWarnings("unchecked")
					ArrayList<Object> features = (ArrayList<Object>) map.get("features");
					int size = features.size();
					if(size <1)
					{
						hasFeatures = false;
					}
				} catch (IOException ex) {
					// In case of an IOException the connection will be
					// released
					// back to the connection manager automatically
					// LOG.error(ex);
					throw ex;
				} catch (RuntimeException ex) {
					// In case of an unexpected exception you may want to
					// abort
					// the HTTP request in order to shut down the underlying
					// connection immediately.
					// LOG.error(ex);
					httpPost.abort();
					throw ex;
				} catch (Exception ex) {

					// LOG.error(ex);
					httpPost.abort();
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
		} catch (Exception e) {

		}
		return hasFeatures;
	}
	
	private void generateEventDef() throws ConfigurationException, GeoEventDefinitionManagerException
	{
		List<FieldDefinition>fdefs = new ArrayList<FieldDefinition>();
		fdefs.add(new DefaultFieldDefinition("trackid", FieldType.String, "TRACK_ID"));
		fdefs.add(new DefaultFieldDefinition("Name",FieldType.String));
		fdefs.add(new DefaultFieldDefinition("FirstStopID", FieldType.Integer));
		fdefs.add(new DefaultFieldDefinition("LastStopID", FieldType.Integer));
		fdefs.add(new DefaultFieldDefinition("StopCount",FieldType.Integer));
		fdefs.add(new DefaultFieldDefinition("Total_Minutes",FieldType.Double));
		fdefs.add(new DefaultFieldDefinition("geometry", FieldType.Geometry, "GEOMETRY"));
		edOut = new DefaultGeoEventDefinition();
		edOut.setName("routing-processor");
		edOut.setOwner(getId());
		edOut.setFieldDefinitions(fdefs);
		manager.addGeoEventDefinition(edOut);
	}
	
	private void generateDirectionsEventDef() throws ConfigurationException, GeoEventDefinitionManagerException
	{
		List<FieldDefinition>fdefs = new ArrayList<FieldDefinition>();
		fdefs.add(new DefaultFieldDefinition("routeid", FieldType.String));
		fdefs.add(new DefaultFieldDefinition("trackid", FieldType.String, "TRACK_ID"));
		fdefs.add(new DefaultFieldDefinition("time", FieldType.Double));
		fdefs.add(new DefaultFieldDefinition("text", FieldType.String));
		fdefs.add(new DefaultFieldDefinition("eta", FieldType.Double));

		dirEdOut = new DefaultGeoEventDefinition();
		dirEdOut.setName("routing-directions");
		dirEdOut.setOwner(getId());
		dirEdOut.setFieldDefinitions(fdefs);
		manager.addGeoEventDefinition(dirEdOut);
	}
	private String createStopsJson(String url) throws UnsupportedEncodingException
	{
		url = url+"/query?";
		String where = "where=1=1";
		url += where + "&f=json";
		String json = "\"type\":\"features\",";
		String initString = "stops=";
		json += "\"url\":\""+url+"\",";
		json += "\"doNotLocateOnRestrictedElements\":true";
		json = encloseCurlyBraces(json);
		json = URLEncoder.encode(json, "UTF-8");
		json = initString + json;
		
		return json;
	}
	
	private String createBarrierJson(String url, BarrierType type) throws ClientProtocolException, IOException
	{
		boolean hasFeatures = true;
		if(!serviceHasFeatures(url))
		{
			hasFeatures=false;
		}
		String initString=null;
		switch(type)
		{
		case POINT:
			initString = "pointBarriers=";
			break;
		case LINE:
			initString = "polylineBarriers=";
			break;
		case POLYGON:
			initString = "polygonBarriers=";
			break;
		}
		if(!hasFeatures)
		{
			return initString;
		}
		url = url+"/query?";
		String where = "where=1=1";
		url += where + "&f=json";
		String json = "\"type\":\"features\",";
		json += "\"url\":\"" + url + "\",";
		json = encloseCurlyBraces(json);
		json = URLEncoder.encode(json, "UTF-8");
		json = initString + json;
		return json;
	}
	
	private String createBaseUrl(String url)
	{
		String json = url + "/solve?";
		return json;
	}
	
	private String encloseCurlyBraces(String inString) throws UnsupportedEncodingException
	{
		return "{" + inString + "}";
	}
	
	private String encloseBrackets(String inString)
	{
		return "[" + inString + "]";
	}
	@Override
	public void disconnect() {
		if (geoEventProducer != null)
			geoEventProducer.disconnect();
		
	}
	@Override
	public EventDestination getEventDestination() {
		return (geoEventProducer != null) ? geoEventProducer
				.getEventDestination() : null;
	}
	@Override
	public String getStatusDetails() {
		return (geoEventProducer != null) ? geoEventProducer.getStatusDetails()
				: "";
	}
	@Override
	public void init() throws MessagingException {
		;
		
	}
	@Override
	public boolean isConnected() {
		return (geoEventProducer != null) ? geoEventProducer.isConnected()
				: false;
	}
	@Override
	public void setup() throws MessagingException {
		;
		
	}
	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void update(Observable o, Object arg) {
		;
		
	}
	@Override
	public void addObserver(Observer arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void deleteObserver(Observer arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public List<EventDestination> getEventDestinations() {
		return (geoEventProducer != null) ? Arrays.asList(geoEventProducer
				.getEventDestination()) : new ArrayList<EventDestination>();
	}
	@Override
	public void send(GeoEvent geoEvent) throws MessagingException {
		if (geoEventProducer != null && geoEvent != null)
			geoEventProducer.send(geoEvent);
		
	}
}
