package com.esri.geoevent.solutions.processor.route;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class RoutingProcessorDefinition extends GeoEventProcessorDefinitionBase{
	private String lblNAService = "${com.esri.geoevent.solutions.processor.route.route-processor.LBL_NA_SERVICE}";
	private String descNAService = "${com.esri.geoevent.solutions.processor.route.route-processor.DESC_NA_SERVICE}";
	private String lblStops = "${com.esri.geoevent.solutions.processor.route.route-processor.LBL_STOPS_SERVICE}";
	private String descStops = "${com.esri.geoevent.solutions.processor.route.route-processor.DESC_STOPS_SERVICE}";
	private String lblPtBarriers = "${com.esri.geoevent.solutions.processor.route.route-processor.LBL_PT_BARRIERS}";
	private String descPtBarriers = "${com.esri.geoevent.solutions.processor.route.route-processor.DESC_PT_BARRIERS}";
	private String lblLineBarriers = "${com.esri.geoevent.solutions.processor.route.route-processor.LBL_LINE_BARRIERS}";
	private String descLineBarriers = "${com.esri.geoevent.solutions.processor.route.route-processor.DESC_LINE_BARRIERS}";
	private String lblPolygonBarriers = "${com.esri.geoevent.solutions.processor.route.route-processor.LBL_POLYGON_BARRIERS}";
	private String descPolygonBarriers = "${com.esri.geoevent.solutions.processor.route.route-processor.DESC_POLYGON_BARRIERS}";
	private String routeProcessorLabel = "${com.esri.geoevent.solutions.processor.route.route-processor.ROUTING_PROCESSOR_LABEL}";
	private String routeProcessorDescription = "${com.esri.geoevent.solutions.processor.route.route-processor.ROUTING_PROCESSOR_DESCRIPTION}";
	private String lblConnection = "${com.esri.geoevent.solutions.processor.route.route-processor.LBL_CONNECTION}";
	private String descConnection = "${com.esri.geoevent.solutions.processor.route.route-processor.DESC_CONNECTION}";
	public RoutingProcessorDefinition() throws PropertyException
	{
		propertyDefinitions.put("connection", new PropertyDefinition("connection", PropertyType.ArcGISConnection, null, lblConnection, descConnection, true, false));
		propertyDefinitions.put("naservice", new PropertyDefinition("naservice", PropertyType.String, "", lblNAService, descNAService, true, false));
		propertyDefinitions.put("stops", new PropertyDefinition("stops", PropertyType.String, "", lblStops, descStops, true, false));
		propertyDefinitions.put("ptbarriers", new PropertyDefinition("ptbarriers", PropertyType.String, "", lblPtBarriers, descPtBarriers, false, false));
		propertyDefinitions.put("linebarriers", new PropertyDefinition("linebarriers", PropertyType.String, "", lblLineBarriers, descLineBarriers, false, false));
		propertyDefinitions.put("polygonbarriers", new PropertyDefinition("polygonbarriers", PropertyType.String, "", lblPolygonBarriers, descPolygonBarriers, false, false));
	}
	
	@Override
	public String getName() {
		return "RoutingProcessor";
	}

	@Override
	public String getDomain() {
		return "com.esri.geoevent.solutions.processor.route";
	}

	@Override
	public String getVersion() {
		return "10.3.1";
	}

	@Override
	public String getLabel() {
		return routeProcessorLabel;
	}

	@Override
	public String getDescription() {
		return routeProcessorDescription;
	}

	@Override
	public String getContactInfo() {
		return "geoeventprocessor@esri.com";
	}
}
