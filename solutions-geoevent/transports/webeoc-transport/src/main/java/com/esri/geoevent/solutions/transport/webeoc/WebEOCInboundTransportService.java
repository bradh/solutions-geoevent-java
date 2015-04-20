package com.esri.geoevent.solutions.transport.webeoc;

import com.esri.ges.adapter.util.XmlAdapterDefinition;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.transport.Transport;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.transport.TransportServiceBase;

public class WebEOCInboundTransportService extends TransportServiceBase {

	public WebEOCInboundTransportService() throws PropertyException
	{
		definition = new WebEOCInboundTransportDefinition();
	}
	@Override
	public Transport createTransport() throws ComponentException {
		return new WebEOCInboundTransport(definition);
	}

}
