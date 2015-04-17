package com.esri.geoevent.solutions.adapter.webeoc;

import com.esri.ges.adapter.AdapterDefinitionBase;
import com.esri.ges.adapter.AdapterType;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;

public class WebEOCInboundAdapterDefinition extends AdapterDefinitionBase {

	public WebEOCInboundAdapterDefinition(AdapterType type) throws PropertyException {
		super(type);
		  propertyDefinitions.put("geoeventDefinition", new PropertyDefinition("geoeventDefinition", PropertyType.String, "","${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_EVENT_DEF}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_EVENT_DEF}", true, false));
		  propertyDefinitions.put("hasGeometry", new PropertyDefinition("hasGeometry", PropertyType.Boolean, false, "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_HAS_GEOMETRY}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_HAS_GEOMETRY}", true, false));
		  propertyDefinitions.put("x", new PropertyDefinition("x", PropertyType.String, "lon", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_GEOMETRY_X}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_GEOMETRY_X}", "hasGeometry=true", true, false));
		  propertyDefinitions.put("y", new PropertyDefinition("y", PropertyType.String, "lat", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_GEOMETRY_Y}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_GEOMETRY_Y}", "hasGeometry=true", true, false));
		  propertyDefinitions.put("useGeocoding", new PropertyDefinition("useGeocoding", PropertyType.Boolean, false, "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_USE_GEOCODING}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_USE_GEOCODING}", "hasGeometry=false", true, false));
		  propertyDefinitions.put("score", new PropertyDefinition("score", PropertyType.Integer, 70, "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_SCORE}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_SCORE}", "useGeocoding=true", false, false));
		  propertyDefinitions.put("geocodeService", new PropertyDefinition("geocodeService", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_GEOCODE_SERVICE}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_GEOCODE_SERVICE}", "useGeocoding=true", false, false));
		  propertyDefinitions.put("geocodeargs", new PropertyDefinition("geocodeargs", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_GEOCODE_ARGUMENTS}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_GEOCODE_ARGUMENTS}", "useGeocoding=true", false, false));
		  //propertyDefinitions.put("geocodeField", new PropertyDefinition("geocodeField", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_GEOCODE_FIELD}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_GEOCODE_FIELD}", "useGeocoding=true", false, false));
		  //propertyDefinitions.put("cityfield", new PropertyDefinition("cityfield", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_CITY_FIELD}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_CITY_FIELD}", "useGeocoding=true", false, false));
		  //propertyDefinitions.put("statefield", new PropertyDefinition("statefield", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_STATE_FIELD}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_STATE_FIELD}", "useGeocoding=true", false, false));
		  //propertyDefinitions.put("countryfield", new PropertyDefinition("countryfield", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_COUNTRY_FIELD}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_COUNTRY_FIELD}", "useGeocoding=true", false, false));
		  //propertyDefinitions.put("zipfield", new PropertyDefinition("zipfield", PropertyType.String, "", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.LBL_ZIPCODE_FIELD}", "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.DESC_ZIPCODE_FIELD}", "useGeocoding=true", false, false));
	}
	@Override
	public String getName() {
		return "WebEOCAdapterIn";
	}

	@Override
	public String getDomain() {
		return "com.esri.geoevent.solutions.adapter.webeoc.inbound";
	}

	@Override
	public String getVersion() {
		return "10.3.0";
	}

	@Override
	public String getLabel() {
		return "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.INBOUND_ADAPTER_LABEL}";
	}

	@Override
	public String getDescription() {
		return "${com.esri.geoevent.solutions.adapter.webeoc.webeoc-adapter.INBOUND_ADAPTER_DESCRIPTION}";
	}

	@Override
	public String getContactInfo() {
		return "geoeventprocessor@esri.com";
  }

}
