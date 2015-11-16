package com.esri.geoevent.solutions.processor.ll2usng;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class USNG2LatLongProcessorDefinition extends GeoEventProcessorDefinitionBase{
	private String lblUSNG = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.LBL_MGRS}";
	private String descUSNG= "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.DESC_MGRS}";
	private String lblGeoField = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.LBL_GEO_FLD}";
	private String descGeoField= "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.DESC_GEO_FLD}";
	private String lblNewDef = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.LBL_NEW_DEF}";
	private String descNewDef = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.DESC_NEW_DEF}";
	private String lblAccuracy = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.LBL_ACCURACY}";
	private String descAccuracy= "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.DESC_ACCURACY}";
	private String lblOverwrite = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.LBL_OVERWRITE}";
	private String descOverwrite= "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.DESC_OVERWRITE}";
	private String lblReturnBB = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.LBL_BB}";
	private String descReturnBB = "${com.esri.geoevent.solutions.processor.ll2usng.ll2usng-processor.DESC_BB}";
	public USNG2LatLongProcessorDefinition() throws PropertyException
	{
		propertyDefinitions.put("usng", new PropertyDefinition("usng", PropertyType.String, "", lblUSNG, descUSNG, true, false));
		propertyDefinitions.put("overwrite", new PropertyDefinition("overwrite", PropertyType.Boolean, true, lblOverwrite, descOverwrite, true, false));
		propertyDefinitions.put("eventdef", new PropertyDefinition("eventdef", PropertyType.String, "", lblNewDef, descNewDef, "overwrite=false", false, false));
		propertyDefinitions.put("geofld", new PropertyDefinition("geofld", PropertyType.String, "", lblGeoField, descGeoField, "overwrite=false", false, false));
		propertyDefinitions.put("returnbb", new PropertyDefinition("returnbb", PropertyType.Boolean, false, lblReturnBB, descReturnBB, true, false));
	}
	@Override
	public String getName() {
		return "USNG2LL";
	}

	@Override
	public String getDomain() {
		return "com.esri.geoevent.solutions.processor.ll2usng";
	}

	@Override
	public String getVersion() {
		return "10.3.1";
	}

	@Override
	public String getLabel() {
		return "USNG to Lat Long Converter";
	}

	@Override
	public String getDescription() {
		return "Converts USNG coordinates to Lat / Lon. Returns southwest corner of USNG or a bounding box ";
	}

	@Override
	public String getContactInfo() {
		return "geoeventprocessor@esri.com";
	}

}
