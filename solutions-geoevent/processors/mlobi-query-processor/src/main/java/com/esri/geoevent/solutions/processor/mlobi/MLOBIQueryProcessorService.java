package com.esri.geoevent.solutions.processor.mlobi;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.http.GeoEventHttpClientService;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class MLOBIQueryProcessorService extends GeoEventProcessorServiceBase{
	private GeoEventDefinitionManager manager;
	private Messaging messaging;
	private GeoEventHttpClientService httpClientService;
	public MLOBIQueryProcessorService() throws PropertyException
	{
		definition = new MLOBIQueryProcessorDefinition();
	}
	@Override
	public GeoEventProcessor create() throws ComponentException {
		MLOBIQueryProcessor mlobiq = new MLOBIQueryProcessor(definition);
		mlobiq.setManager(manager);
		mlobiq.setMessaging(messaging);
		mlobiq.setHttpClientService(httpClientService);
		return mlobiq;
	}
	
	public void setManager(GeoEventDefinitionManager manager) {
		this.manager = manager;
	}

	public void setMessaging(Messaging messaging) {
		this.messaging = messaging;
	}
	
	public void setHttpClientService( GeoEventHttpClientService service )
	{
	  this.httpClientService = service;
	}

}
