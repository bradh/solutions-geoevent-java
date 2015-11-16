package com.esri.geoevent.solutions.processor.ll2usng;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class LatLong2USNGProcessorService extends GeoEventProcessorServiceBase {
	private GeoEventDefinitionManager manager;
	public Messaging messaging;
	public LatLong2USNGProcessorService()
	{
			definition = new LatLong2USNGProcessorDefinition();
	}
	public GeoEventProcessor create() throws ComponentException {
		LatLong2USNGProcessor processor =  new LatLong2USNGProcessor(definition);
		processor.setManager(manager);
		processor.setMessaging(messaging);
		return processor;
	}
	
	public void setManager(GeoEventDefinitionManager manager)
	{
		this.manager =  manager;
	}
	
	public void setMessaging(Messaging messaging)
	{
		this.messaging = messaging;
	}
	


}
