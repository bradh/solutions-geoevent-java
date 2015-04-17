package com.esri.geoevent.solutions.adapter.webeoc;

import com.esri.ges.adapter.Adapter;
import com.esri.ges.adapter.AdapterServiceBase;
import com.esri.ges.adapter.AdapterType;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.property.PropertyException;

public class WebEOCInboundAdapterService extends AdapterServiceBase {
	public WebEOCInboundAdapterService() throws PropertyException
	{
		definition = new WebEOCInboundAdapterDefinition(AdapterType.INBOUND);
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
