package com.esri.geoevent.solutions.processor.hotspot;

import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class HotspotProcessorDefinition extends GeoEventProcessorDefinitionBase {
	//properties interval, intervaltype, minsize (events collection), 
	private String lblInterval = "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_INTERVAL}";
	private String descInterval = "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_INTERVAL}";
	private String lblIntervalIsTime = "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_INTERVAL_IS_TIME}";
	private String descIntervalIsTime = "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_INTERVAL_IS_TIME}";
	private String lblMinSize = "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_MIN_SIZE}";
	private String descMinSize = "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_MIN_SIZE}";
	private String lblXMin= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_XMIN}";
	private String descXMin= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_XMIN}";
	private String lblYMin= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_YMIN}";
	private String descYMin= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_YMIN}";
	private String lblXMax= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_XMAX}";
	private String descXMax= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_XMAX}";
	private String lblYMax= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_YMAX}";
	private String descYMax= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_YMAX}";
	private String lblWkid= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.LBL_WKID}";
	private String descWkid= "${com.esri.geoevent.solutions.processor.hotspot.hotspot-processor.DESC_WKID}";
	
	HotspotProcessorDefinition() throws PropertyException
	{
		PropertyDefinition pdInterval = new PropertyDefinition("interval", PropertyType.Integer, 60000, lblInterval, descInterval, true, false);
		propertyDefinitions.put(pdInterval.getPropertyName(), pdInterval);
		PropertyDefinition pdIntervalTime = new PropertyDefinition("intervalIsTime", PropertyType.Boolean, true, lblIntervalIsTime, descIntervalIsTime, true, false);
		propertyDefinitions.put(pdIntervalTime.getPropertyName(), pdIntervalTime);
		PropertyDefinition pdMinSize = new PropertyDefinition("minsize", PropertyType.Integer, 1000, lblMinSize, descMinSize, true, false);
		propertyDefinitions.put(pdMinSize.getPropertyName(), pdMinSize);
		PropertyDefinition pdXMin = new PropertyDefinition("xmin", PropertyType.Double, 0.0, lblXMin, descXMin, true, false);
		propertyDefinitions.put(pdXMin.getPropertyName(), pdXMin);
		PropertyDefinition pdYMin = new PropertyDefinition("ymin", PropertyType.Double, 0.0, lblYMin, descYMin, true, false);
		propertyDefinitions.put(pdYMin.getPropertyName(), pdYMin);
		PropertyDefinition pdXMax = new PropertyDefinition("xmax", PropertyType.Double, 0.0, lblXMax, descXMax, true, false);
		propertyDefinitions.put(pdXMax.getPropertyName(), pdXMax);
		PropertyDefinition pdYMax = new PropertyDefinition("ymax", PropertyType.Double, 0.0, lblYMax, descYMax, true, false);
		propertyDefinitions.put(pdYMax.getPropertyName(), pdYMax);
		PropertyDefinition pdWkid = new PropertyDefinition("wkid", PropertyType.Integer, 0.0, lblWkid, descWkid, true, false);
		propertyDefinitions.put(pdWkid.getPropertyName(), pdWkid);
	}
	
}
