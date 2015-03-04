package com.esri.geoevent.solutions.adapter.eventJson;

import com.esri.ges.adapter.Adapter;
import com.esri.ges.adapter.AdapterServiceBase;
import com.esri.ges.adapter.util.XmlAdapterDefinition;
import com.esri.ges.core.component.ComponentException;

public class EventJsonInboundAdapterService extends AdapterServiceBase {

	public EventJsonInboundAdapterService()
	{
		definition = new XmlAdapterDefinition(getResourceAsStream("inbound-adapter-definition.xml"));
	}

	@Override
	public Adapter createAdapter() throws ComponentException
	{
		return new EventJsonInboundAdapter(definition);
	}
	
	

}
