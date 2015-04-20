package com.esri.geoevent.solutions.transport.webeoc;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.transport.TransportDefinitionBase;
import com.esri.ges.transport.TransportType;

public class WebEOCInboundTransportDefinition extends TransportDefinitionBase {

	public WebEOCInboundTransportDefinition() throws PropertyException {
		super(TransportType.INBOUND);
		propertyDefinitions.put("webeocurl", new PropertyDefinition("webeocurl", PropertyType.String, "", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_WEBEOC_URL}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_WEBEOC_URL}", true, false ));
		propertyDefinitions.put("incident", new PropertyDefinition("incident", PropertyType.String, "","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_INCIDENT}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_INCIDENT}", true, false ));
		propertyDefinitions.put("user", new PropertyDefinition("user", PropertyType.String, "", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_USERNAME}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_USERNAME}", true, false ));
		propertyDefinitions.put("password", new PropertyDefinition("password", PropertyType.String, "", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_PASSWORD}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_PASSWORD}", true, false ));
		propertyDefinitions.put("position", new PropertyDefinition("position", PropertyType.String, "", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_POSITION}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_POSITION}", true, false ));
		propertyDefinitions.put("board", new PropertyDefinition("board", PropertyType.String, "", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_BOARD}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_BOARD}", true, false ));
		propertyDefinitions.put("view", new PropertyDefinition("view", PropertyType.String, "", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_VIEW}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_VIEW}", true, false ));
		propertyDefinitions.put("pollingInterval", new PropertyDefinition("pollingInterval", PropertyType.Integer, "60000", "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.LBL_POLLING_INTERVAL}","${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.DESC_POLLING_INTERVAL}", true, false ));
	
	}
	@Override
	public String getName()
	{
		return "WebEOCTransportIn";
	}
	
	@Override
  public String getLabel()
  {
    return "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.INBOUND_TRANSPORT_LABEL}";
  }
	
	@Override
	public String getDomain()
	{
		return "com.esri.geoevent.solutions.transport.webeoc.inbound";
	}

	@Override
	public String getDescription()
	{
		return "${com.esri.geoevent.solutions.transport.webeoc.webeoc-transport.INBOUND_TRANSPORT_DESCRIPTION}";
	}
	@Override
	public String getVersion()
	{
		return "10.3.0";
	}
}
