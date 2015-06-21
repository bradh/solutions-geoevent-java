package com.esri.geoevent.solutions.processor.mlobi;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class MLOBIQueryProcessorDefinition extends GeoEventProcessorDefinitionBase{
	public MLOBIQueryProcessorDefinition() throws PropertyException
	{
		propertyDefinitions.put("host", new PropertyDefinition("host", PropertyType.String, "", "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LBL_SERVER}","${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.DESC_SERVER}", true, false));
		propertyDefinitions.put("user", new PropertyDefinition("user", PropertyType.String, "", "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LBL_USER}","${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.DESC_USER}", true, false));
		propertyDefinitions.put("password", new PropertyDefinition("password", PropertyType.String, "", "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LBL_PW}","${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.DESC_PW}", true, false));
		propertyDefinitions.put("query", new PropertyDefinition("query", PropertyType.Condition, null, "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LBL_QUERY}","${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.DESC_QUERY}", true, false));
	} 
	
	@Override
	public String getName() {
		return "MLOBIQueryProcessor";
	}

	@Override
	public String getDomain() {
		return "com.esri.geoevent.solutions.processor.mlobi";
	}

	@Override
	public String getVersion() {
		return "10.3.1";
	}

	@Override
	public String getLabel() {
		return "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.PROCESSOR_LABEL}";
	}

	@Override
	public String getDescription() {
		return "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor-processor.PROCESSOR_DESCRIPTION}";
	}

	@Override
	public String getContactInfo() {
		return "geoeventprocessor@esri.com";
	}

}
