package com.esri.geoevent.solutions.processor.visibility;

/*
 * #%L
 * Esri :: AGES :: Solutions :: Processor :: Geometry
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2014 Esri
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.ges.core.ConfigurationException;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;


public class VisibilityProcessor extends GeoEventProcessorBase {
	private static final Log LOG = LogFactory.getLog(VisibilityProcessor.class);
	public GeoEventDefinitionManager manager;
	private SpatialReference srIn;
	private SpatialReference srBuffer;
	private SpatialReference srOut;
	private String gp;
	private String is;
	private String radiusSource;
	private Boolean isRadiusConstant = false;
	private Double radiusConstant;
	private String radiusEventfld;
	private String radiusUnit;
	private String elevationSource;
	private String elevEventfld;
	private double elevConstant;
	private Boolean isElevConstant=false;
	private String units_elev;
	int inwkid;
	int outwkid;
	int procwkid;
	public VisibilityProcessor(GeoEventProcessorDefinition definition, GeoEventDefinitionManager m)
			throws ComponentException {
		super(definition);
		manager = m;
		//tagMgr=tm;
		geoEventMutator= true;
	}
	@Override 
	public void afterPropertiesSet()
	{
		gp = properties.get("gpservice").getValue().toString();
		is = properties.get("imageservice").getValue().toString();
		radiusSource = properties.get("radiusSource").getValue().toString();
		if(radiusSource.equals("Constant"))
		{
			isRadiusConstant=true;
			radiusConstant = (Double) properties.get("radius").getValue();
		}
		else
		{
			radiusEventfld = properties.get("radiusEvent").getValue().toString();
		}
		radiusUnit = properties.get("units").getValue().toString();
		elevationSource = properties.get("elevationSource").getValue().toString();
		if(elevationSource.equals("Constant"))
		{
			isElevConstant=true;
			elevConstant = (Double)properties.get("elevation").getValue();
		}
		units_elev = properties.get("units_elev").getValue().toString();
		elevEventfld = properties.get("elevationEvent").getValue().toString();
		//inwkid = (Integer) properties.get("wkidin").getValue();
		outwkid = (Integer) properties.get("wkidout").getValue();
		procwkid = (Integer) properties.get("wkidbuffer").getValue();
	}
	
	@Override
	public synchronized void validate() throws ValidationException
	{
		super.validate();
		try
		{
			//srIn = SpatialReference.create(inwkid);
			srBuffer = SpatialReference.create(procwkid);
			srOut = SpatialReference.create(outwkid);
		}
		catch(Exception e)
		{
			ValidationException ve = new ValidationException("Invalid wkid");
			LOG.error(e.getMessage());
			LOG.error(ve.getMessage());
			throw ve;
		}
	}
	
	@Override
	public GeoEvent process(GeoEvent ge) throws Exception {
		double radius;
		if(!ge.getGeoEventDefinition().getTagNames().contains("GEOMETRY"))
		{
			return null;
		}
		srIn=ge.getGeometry().getSpatialReference();
		inwkid=srIn.getID();
		if(isRadiusConstant)
		{
			radius = radiusConstant;
		}
		else
		{
			
			radius = (Double)ge.getField(radiusEventfld);
		}
		
		double elevation;
		
		if(isElevConstant){
			elevation =elevConstant;
		}
		else
		{
			elevation = (Double)ge.getField(elevEventfld);
		}
		ConstructVisibilityRest(ge, gp, is, radius, radiusUnit,  elevation, units_elev, procwkid);
		return ge;
	}
	
	private void ConstructVisibilityRest(GeoEvent ge, String gpservice, String imageservice, double range, String unit,  double elevation, String units_elev, int wkid) throws UnsupportedEncodingException, IOException, ConfigurationException, FieldException 
	{
		UnitConverter uc = new UnitConverter();
		range = uc.Convert(range, unit, srBuffer);
		String procUnitName = srBuffer.getUnit().getName();
		// normalize horizontal and vertical units to z-factor = 1
		// double inElev = elevation;
		if (procUnitName.equals("Meter")) {
			if (units_elev.equals("Feet")) {
				elevation = elevation * 0.3048;
			}
		} else {
			if (units_elev.equals("Meters")) {
				elevation = elevation * 3.28084;
			}
		}
		
		MapGeometry eventGeo = ge.getGeometry();
		
		Geometry tmpmask = eventGeo.getGeometry();
		Geometry mask = GeometryEngine.project(tmpmask, srIn, srBuffer);
		Envelope extent = new Envelope();
		String obs = "";
		if(properties.get("observerSource").getValueAsString().equals("Geoevent"))
		{
			mask.queryEnvelope(extent);
			Double x = extent.getCenterX();
			Double y = extent.getCenterY();

			String cx = ((Double) x).toString();
			String cy = ((Double) y).toString();
			obs = cx + " " + cy;
		}
		else if(properties.get("observerSource").getValueAsString().equals("Field"))
		{
			
			String xeventfld = properties.get("observerXEvent").getValue().toString();
			String[] arr = xeventfld.split(":");
			Double x = (Double)ge.getField(arr[1]);
			String yeventfld = properties.get("observerYEvent").getValue().toString();
			arr = yeventfld.split(":");
			Double y = (Double)ge.getField(arr[1]);
			Point p = new Point(x,y);
			p = (Point)GeometryEngine.project(p, srIn, srBuffer);
			obs = ((Double)p.getX()).toString() + " " + ((Double)p.getY()).toString();
		}
		else
		{
			Double x = (Double)properties.get("observerX").getValue();
			Double y = (Double)properties.get("observerY").getValue();
			Point p = new Point(x,y);
			p = (Point)GeometryEngine.project(p, srIn, srBuffer);
			obs = ((Double)p.getX()).toString() + " " + ((Double)p.getY()).toString();
		}
		String contentType = "application/json";
		HttpClient httpclient = HttpClientBuilder.create().build();
		String observers = URLEncoder.encode(obs, "UTF-8");
		imageservice = URLEncoder.encode(imageservice, "UTF-8");
		String jsonGeo = URLEncoder.encode(
				GeometryEngine.geometryToJson(srBuffer, mask), "UTF-8");

		String args = "observers=" + observers + "&image_service_url="
				+ imageservice + "&radius=" + ((Double) range).toString()
				+ "&height=" + ((Double) elevation).toString() + "&json_mask="
				+ jsonGeo + "&wkid=" + ((Integer) wkid).toString() + "&f=json";
		String path = gpservice + "/execute?";
		String uri = path + args;
		MapGeometry visible = null;
		MapGeometry nonvisible = null;
		try {
			HttpPost httppost = new HttpPost(uri);
			httppost.setHeader("Accept", contentType);
			HttpResponse response = httpclient.execute(httppost);

			HttpEntity entity = response.getEntity();
			
			if (entity != null) {
				InputStream instream = entity.getContent();
				try {
					// instream.read();
					BufferedReader br = new BufferedReader(
							new InputStreamReader((instream)));
					String output = "";
					String ln;
					while ((ln = br.readLine()) != null) {
						output += ln;
					}
					
					ObjectMapper mapper = new ObjectMapper();
					@SuppressWarnings("unchecked")
					Map<String, Object> map = (Map<String, Object>)mapper.readValue(output, new TypeReference<HashMap<String, Object>>(){});
					@SuppressWarnings("unchecked")
					ArrayList<Object> resString = (ArrayList<Object>)map.get("results");
					
					@SuppressWarnings("unchecked")
					Map<String, Object> r = (Map<String, Object>)resString.get(0);
					@SuppressWarnings("unchecked")
					Map<String,Object> fset = (Map<String,Object>)r.get("value");
					//String fsetJson = mapper.writeValueAsString(val);
					List<HashMap<String, Object>> features = (ArrayList<HashMap<String,  Object>>)fset.get("features");
					JsonFactory jf = new JsonFactory();
					JsonParser jp = null;
					for (HashMap<String, Object> feature : features) {

						HashMap<String, Object> attributes = (HashMap<String, Object>) feature.get("attributes");
						int code = (Integer) attributes.get(
								"gridcode");
						// com.esri.ges.spatial.Geometry tmpgesVis = null;
						// com.esri.ges.spatial.Geometry tmpgesNonVis =
						// null;
						if (code == 0) {
							String geoString = feature.get("geometry").toString();
							jp = jf.createJsonParser(geoString);
							MapGeometry tmpMapGeoVis = GeometryEngine.jsonToGeometry(jp);
							Geometry tmpvis = tmpMapGeoVis.getGeometry();
							Geometry vis = GeometryEngine.project(tmpvis,
									srBuffer, srOut);
							visible = new MapGeometry(vis, srOut);

						} else {
							String geoString = feature.get("geometry").toString();
							jp = jf.createJsonParser(geoString);
							MapGeometry tmpMapGeoNonVis = GeometryEngine.jsonToGeometry(jp);
							Geometry tmpnonvis = tmpMapGeoNonVis.getGeometry();
							Geometry nonvis = GeometryEngine.project(
									tmpnonvis, srBuffer, srOut);
							nonvisible = new MapGeometry(nonvis, srOut);

						}
					}
					
				} catch (IOException ex) {
					// In case of an IOException the connection will be
					// released
					// back to the connection manager automatically
					throw ex;
				} catch (RuntimeException ex) {
					// In case of an unexpected exception you may want to
					// abort
					// the HTTP request in order to shut down the underlying
					// connection immediately.
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
		}
		catch(Exception ex){}
		/*GeoEventDefinition geoDef = ge.getGeoEventDefinition();
		List<FieldDefinition>fieldDefs = geoDef.getFieldDefinitions();
		ArrayList<FieldDefinition> newFieldDefs = new ArrayList<FieldDefinition>();
		for(FieldDefinition fieldDef: fieldDefs)
		{
			newFieldDefs.add(fieldDef);
		}
		FieldDefinition visFldDef = new DefaultFieldDefinition("visible", FieldType.Geometry, (String)null);
		FieldDefinition nonvisFldDef = new DefaultFieldDefinition("nonvisible", FieldType.Geometry, (String)null);
		newFieldDefs.add(visFldDef);
		newFieldDefs.add(nonvisFldDef);
		geoDef.setFieldDefinitions(newFieldDefs);*/
		ge.setField("visible", visible);
		ge.setField("nonvisible", nonvisible);
		ge.setField("gcvis", new Integer(1));
		ge.setField("gcnvis", new Integer(0));
	}
	
	
		
	private String ConstructJsonMaskFromGeoEvent(GeoEvent ge) throws IOException
	{
		MapGeometry eventgeo = ge.getGeometry();
		Geometry geo = eventgeo.getGeometry();
		Geometry maskGeo = GeometryEngine.project(geo, srIn, srBuffer);
		return GeometryEngine.geometryToJson(srBuffer, maskGeo);
	}
	
	

}
