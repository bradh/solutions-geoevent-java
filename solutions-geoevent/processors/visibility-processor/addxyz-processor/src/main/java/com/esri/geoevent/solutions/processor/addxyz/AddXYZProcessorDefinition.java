package com.esri.geoevent.solutions.processor.addxyz;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class AddXYZProcessorDefinition extends GeoEventProcessorDefinitionBase {
	private String lblGeoField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.LBL_GEOMETRY_FIELD}";
	private String descGeoField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.DESC_GEOMETRY_FIELD}";
	private String lblEvent = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.LBL_GEOEVENT_DEF_NAME}";
	private String descEvent = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.DESC_GEOEVENT_DEF_NAME}";
	private String lblXField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.LBL_X_FIELD}";
	private String descXField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.DESC_X_FIELD}";
	private String lblYField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.LBL_Y_FIELD}";
	private String descYField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.DESC_Y_FIELD}";
	private String lblZField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.LBL_Z_FIELD}";
	private String descZField = "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.DESC_Z_FIELD}";
	public AddXYZProcessorDefinition() throws PropertyException {
		PropertyDefinition pdGeometryFieldName = new PropertyDefinition("geofield", PropertyType.String, "", lblGeoField, descGeoField, true, false);
		propertyDefinitions.put(pdGeometryFieldName.getPropertyName(), pdGeometryFieldName);
		
		PropertyDefinition pdGEDName = new PropertyDefinition("gedName", PropertyType.String, "", lblEvent, descEvent, true, false);
		propertyDefinitions.put(pdGEDName.getPropertyName(), pdGEDName);
		
		PropertyDefinition pdXField = new PropertyDefinition("xfield", PropertyType.String, "x", lblXField, descXField, true, false);
		propertyDefinitions.put(pdXField.getPropertyName(), pdXField);
		
		PropertyDefinition pdYField = new PropertyDefinition("yfield", PropertyType.String, "y", lblYField, descYField, true, false);
		propertyDefinitions.put(pdYField.getPropertyName(), pdYField);
		
		PropertyDefinition pdZField = new PropertyDefinition("zfield", PropertyType.String, "z", lblZField, descZField, false, false);
		propertyDefinitions.put(pdZField.getPropertyName(), pdZField);
	}
	
	@Override
	public String getName() {
		return "AddXYZProcessor";
	}

	@Override
	public String getDomain() {
		return "com.esri.geoevent.solutions.processor.addxyz";
	}

	@Override
	public String getVersion() {
		return "10.3.0";
	}

	@Override
	public String getLabel() {
		return "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.PROCESSOR_LABEL}";
	}

	@Override
	public String getDescription() {
		return "${com.esri.geoevent.solutions.processor.addxyz.addxyz-processor.PROCESSOR_DESCRIPTION}";
	}

	@Override
	public String getContactInfo() {
		return "geoeventprocessor@esri.com";
	}


}
