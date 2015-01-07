package com.esri.geoevent.solutions.processor.hotspot;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class HotspotProcessorService extends GeoEventProcessorServiceBase {
	public GeoEventDefinitionManager manager;
	public Messaging messaging;

	@Override
	public GeoEventProcessor create() throws ComponentException {

		HotspotProcessor hotspotProc = new HotspotProcessor(definition);
		hotspotProc.setManager(manager);
		hotspotProc.setMessaging(messaging);
		return(hotspotProc);

	}

	// Getters Setters
	public void setManager(GeoEventDefinitionManager manager) {
		this.manager = manager;
	}

	public void setMessaging(Messaging messaging) {
		this.messaging = messaging;
	}

}
