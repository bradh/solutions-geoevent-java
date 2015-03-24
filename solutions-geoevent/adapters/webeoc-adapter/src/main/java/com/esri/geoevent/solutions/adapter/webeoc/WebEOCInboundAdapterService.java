package com.esri.geoevent.solutions.adapter.webeoc;

import com.esri.ges.adapter.Adapter;
import com.esri.ges.adapter.AdapterServiceBase;
import com.esri.ges.adapter.util.XmlAdapterDefinition;
import com.esri.ges.core.component.ComponentException;

public class WebEOCInboundAdapterService extends AdapterServiceBase {
	public WebEOCInboundAdapterService()
	{
		definition = new XmlAdapterDefinition(getResourceAsStream("inbound-adapter-definition.xml"));
	}
	@Override
	public Adapter createAdapter() throws ComponentException {
		try
		{
			return new WebEOCInboundAdapter(definition);
		}
		catch (Exception e)
		{
			throw new ComponentException("WebEOCInboundAdapter instantiation failed: " + e.getMessage());
		}
	}

}
