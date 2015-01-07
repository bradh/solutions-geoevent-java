package com.esri.geoevent.solutions.processor.hotspot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Observable;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.property.Property;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

public class HotspotProcessor extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable{
	private final static String GEOEVENT_INPUT_SPOUT = "geoevent-input-spout";
	private final static String EXTRACT_GEOMETRY_BOLT = "extract-geometry-bolt";
	private final static String COLLECT_POINTS_BOLT = "collect-points-bolt";
	private final static String GENERATE_NEAREST_NEIGHBOR_BOLT = "generate-nearest-neighbor-bolt";
	private final static String FIND_CELL_SIZE_BOLT = "find-cell-size-bolt";
	private final static String GENERATE_GRID_PARAMS_BOLT = "generate-grid-bolt";
	private final static String GENERATE_CELL_GEOMETRY_BOLT = "generate-cell-geometry-bolt";
	private final static String GENERATE_FISHNET_BOLT = "generate-fishnet-bolt";
	private final static String JOIN_CELL_POINT_BOLT = "join-cell-point-bolt";
	private final static String FIND_POINTS_IN_CELL_BOLT = "find-points-in-cell-bolt";
	private final static String AGGREGATE_POINTS_IN_CELLS_BOLT = "aggregate-points-in-cells-bolt";
	private final static String GENERATE_HOTSPOT_EVENT_BOLT="generate-hotspot-event-bolt";
	
	public GeoEventDefinitionManager manager;
	public Messaging messaging;
	private GeoEventCreator geoEventCreator;
	private GeoEventProducer geoEventProducer;
	//processor properties
	private String intervalType = "NUM_NEW_EVENTS";
	private Integer interval;
	private Integer minSize;
	private String outDefNamePrefix;
	private Double xmin;
	private Double ymin;
	private Double xmax;
	private Double ymax;
	private Integer wkid;
	
	private GeoEventInputSpout inputSpout;
	private ExtractGeometryBolt egBolt;
	private CollectPointsBolt cpBolt;
	private GenerateNearestNeighborBolt gnnBolt;
	private FindCellSizeBolt fcsBolt;
	private GenerateGridParamsBolt ggpBolt;
	private GenerateCellGeometryBolt gcgBolt;
	private GenerateFishnetBolt gfBolt;
	private JoinCellPointBolt jcpBolt;
	private FindPointsInCellBolt fpcBolt;
	private AggregatePointsInCellsBolt apcBolt;
	private GenerateHotspotEventBolt gheBolt;
	
	public LinkedBlockingQueue<GeoEvent> queue = new LinkedBlockingQueue<GeoEvent>();
	
	public HotspotProcessor(GeoEventProcessorDefinition definition)
			throws ComponentException {
		super(definition);
	}
	
	
	@Override
	public void setId(String id) {
		super.setId(id);
		geoEventProducer = messaging
				.createGeoEventProducer(new EventDestination(id + ":event"));
	}

	@Override
	public void send(GeoEvent geoEvent) throws MessagingException {
		if (geoEventProducer != null && geoEvent != null)
			geoEventProducer.send(geoEvent);
	}
	
	@Override
	public EventDestination getEventDestination()
	{
		return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
	}
	
	@Override
	public List<EventDestination> getEventDestinations()
	{
		return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<EventDestination>();
	}

	@Override
	public void disconnect()
	{
		if (geoEventProducer != null)
      geoEventProducer.disconnect();
	}

	@Override
	public boolean isConnected()
	{
		return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
	}

	@Override
	public String getStatusDetails()
	{
		return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
	}
	
	@Override
	public void setup() throws MessagingException
	{
		;
	}

	@Override
	public void init() throws MessagingException
	{
		;
	}
	
	@Override
	public void update(Observable o, Object arg)
	{
		;
	}
	
	@Override
	public GeoEvent process(GeoEvent event) throws Exception {
		inputSpout.pushEvent(event);
		while(!queue.isEmpty())
		{
			GeoEvent ge = queue.poll();
			send(ge);
		}
		return null;
	}
	
	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(definition.getName());
		sb.append("/");
		sb.append(definition.getVersion());
		sb.append("[");
		for (Property p : getProperties())
		{
			sb.append(p.getDefinition().getPropertyName());
			sb.append(":");
			sb.append(p.getValue());
			sb.append(" ");
		}
		sb.append("]");
		return sb.toString();
	}
	
	@Override
	public void afterPropertiesSet()
	{
		interval = (Integer)properties.get("interval").getValue();
		minSize = (Integer)properties.get("minsize").getValue();
		outDefNamePrefix = properties.get("outdefnameprefix").getValueAsString();
		xmin = (Double)properties.get("xmin").getValue();
		ymin = (Double)properties.get("ymin").getValue();
		xmax = (Double)properties.get("xmax").getValue();
		ymax = (Double)properties.get("ymax").getValue();
		wkid = (Integer)properties.get("wkid").getValue();
		
		if((Boolean)properties.get("intervalIsTime").getValue())
		{
			intervalType = "TIME";
		}
		
		inputSpout = new GeoEventInputSpout();
		inputSpout.setGeoEventDefinition((GeoEventProcessorDefinition) definition);
		
		egBolt = new ExtractGeometryBolt();
		cpBolt = new CollectPointsBolt(intervalType, interval, minSize);
		gnnBolt = new GenerateNearestNeighborBolt();
		fcsBolt = new FindCellSizeBolt();
		ggpBolt = new GenerateGridParamsBolt(xmin, ymin, xmax, ymax, wkid);
		gcgBolt = new GenerateCellGeometryBolt();
		gfBolt = new GenerateFishnetBolt();
		jcpBolt = new JoinCellPointBolt();
		fpcBolt = new FindPointsInCellBolt();
		apcBolt = new AggregatePointsInCellsBolt();
		gheBolt = new GenerateHotspotEventBolt(outDefNamePrefix);
		gheBolt.setProcessor(this);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(GEOEVENT_INPUT_SPOUT, inputSpout);
		builder.setBolt(EXTRACT_GEOMETRY_BOLT, egBolt).shuffleGrouping(GEOEVENT_INPUT_SPOUT);
		builder.setBolt(COLLECT_POINTS_BOLT, cpBolt).globalGrouping(EXTRACT_GEOMETRY_BOLT);
		builder.setBolt(GENERATE_NEAREST_NEIGHBOR_BOLT, gnnBolt).fieldsGrouping(COLLECT_POINTS_BOLT, "processStream", new Fields("procId"));
		builder.setBolt(FIND_CELL_SIZE_BOLT, fcsBolt).fieldsGrouping(GENERATE_NEAREST_NEIGHBOR_BOLT, new Fields("procId"));
		builder.setBolt(GENERATE_GRID_PARAMS_BOLT , ggpBolt).fieldsGrouping(FIND_CELL_SIZE_BOLT, new Fields("procId"));
		builder.setBolt(GENERATE_CELL_GEOMETRY_BOLT, gcgBolt).shuffleGrouping(GENERATE_GRID_PARAMS_BOLT);
		builder.setBolt(GENERATE_FISHNET_BOLT, gfBolt).fieldsGrouping(GENERATE_CELL_GEOMETRY_BOLT, new Fields("procId"));
		builder.setBolt(JOIN_CELL_POINT_BOLT, jcpBolt).fieldsGrouping(COLLECT_POINTS_BOLT, "pointsStream", new Fields("procId")).fieldsGrouping(GENERATE_FISHNET_BOLT, new Fields("procId"));
		builder.setBolt(FIND_POINTS_IN_CELL_BOLT, fpcBolt).shuffleGrouping(JOIN_CELL_POINT_BOLT);
		builder.setBolt(AGGREGATE_POINTS_IN_CELLS_BOLT, apcBolt).fieldsGrouping(FIND_POINTS_IN_CELL_BOLT, new Fields("procId")).fieldsGrouping(COLLECT_POINTS_BOLT, "ownerStream", new Fields("procId"));
		builder.setBolt(GENERATE_HOTSPOT_EVENT_BOLT, gheBolt).shuffleGrouping(AGGREGATE_POINTS_IN_CELLS_BOLT);
	}
	
	//Getters Setters
	public void setManager(GeoEventDefinitionManager manager)
	{
		this.manager=manager;
	}
	
	public void setMessaging(Messaging messaging)
	{
		this.messaging=messaging;
		this.geoEventCreator = messaging.createGeoEventCreator();
	}
	
	public LinkedBlockingQueue<GeoEvent> getEventQueue()
	{
		return queue;
	}
	
}
