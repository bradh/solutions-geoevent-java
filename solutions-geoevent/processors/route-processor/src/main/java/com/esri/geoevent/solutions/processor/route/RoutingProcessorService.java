package com.esri.geoevent.solutions.processor.route;


import java.util.List;
import java.util.Observable;
import java.util.Observer;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.manager.datastore.agsconnection.ArcGISServerConnectionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class RoutingProcessorService extends GeoEventProcessorServiceBase{
	private ArcGISServerConnectionManager connectionManager;
	private Messaging messaging;
	private GeoEventDefinitionManager manager;
	public RoutingProcessorService() throws PropertyException
	{
		definition = new RoutingProcessorDefinition();
	}
	public GeoEventProcessor create() throws ComponentException {
		RoutingProcessor processor = new RoutingProcessor(definition);
		processor.setConnectionManager(connectionManager);
		processor.setMessaging(messaging);
		processor.setManager(manager);
		return processor;
	}
	
	//getters setters
	public void setConnectionManager(ArcGISServerConnectionManager cm) {
		connectionManager = cm;
	}
	
	public void setMessaging(Messaging m)
	{
		messaging = m;
	}
	
	public void setManager(GeoEventDefinitionManager mgr)
	{
		manager = mgr;
	}
	
	
	
	

}
