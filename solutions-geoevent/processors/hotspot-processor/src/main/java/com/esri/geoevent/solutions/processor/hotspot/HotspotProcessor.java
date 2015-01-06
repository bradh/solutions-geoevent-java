package com.esri.geoevent.solutions.processor.hotspot;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.esri.core.geometry.Envelope;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

public class HotspotProcessor extends GeoEventProcessorBase {
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
	//processor properties
	private String intervalType = "NUM_NEW_EVENTS";
	private Integer interval;
	private Integer minSize;
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
	
	
	public HotspotProcessor(GeoEventProcessorDefinition definition)
			throws ComponentException {
		super(definition);
		// TODO Auto-generated constructor stub
	}

	@Override
	public GeoEvent process(GeoEvent arg0) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void AfterPropertiesSet()
	{
		interval = (Integer)properties.get("interval").getValue();
		minSize = (Integer)properties.get("minsize").getValue();
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
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(GEOEVENT_INPUT_SPOUT, inputSpout);
		builder.setBolt(EXTRACT_GEOMETRY_BOLT, egBolt).shuffleGrouping(GEOEVENT_INPUT_SPOUT);
		builder.setBolt(COLLECT_POINTS_BOLT, cpBolt).globalGrouping(EXTRACT_GEOMETRY_BOLT);
		builder.setBolt(GENERATE_NEAREST_NEIGHBOR_BOLT, gnnBolt).fieldsGrouping(COLLECT_POINTS_BOLT, "processStream", new Fields("procId"));
		builder.setBolt(FIND_CELL_SIZE_BOLT, fcsBolt).fieldsGrouping(GENERATE_NEAREST_NEIGHBOR_BOLT, new Fields("procId"));
		builder.setBolt(GENERATE_GRID_PARAMS_BOLT , ggpBolt).fieldsGrouping(FIND_CELL_SIZE_BOLT, new Fields("procId"));
		builder.setBolt(GENERATE_CELL_GEOMETRY_BOLT, gcgBolt).shuffleGrouping(GENERATE_GRID_PARAMS_BOLT);
		builder.setBolt(GENERATE_FISHNET_BOLT, gfBolt).fieldsGrouping(GENERATE_CELL_GEOMETRY_BOLT, new Fields("procId"));
		builder.setBolt(JOIN_CELL_POINT_BOLT, jcpBolt).fieldsGrouping(COLLECT_POINTS_BOLT, new Fields("procId")).fieldsGrouping(GENERATE_FISHNET_BOLT, new Fields("procId"));
		builder.setBolt(FIND_POINTS_IN_CELL_BOLT, fpcBolt).shuffleGrouping(JOIN_CELL_POINT_BOLT);
		builder.setBolt(AGGREGATE_POINTS_IN_CELLS_BOLT, apcBolt).fieldsGrouping(FIND_POINTS_IN_CELL_BOLT, new Fields("procId"));
	}

}
