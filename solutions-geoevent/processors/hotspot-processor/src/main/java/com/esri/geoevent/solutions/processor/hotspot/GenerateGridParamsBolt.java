package com.esri.geoevent.solutions.processor.hotspot;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GenerateGridParamsBolt extends BaseRichBolt {
	private OutputCollector collector;
	private static final Log LOG = LogFactory.getLog(GenerateGridParamsBolt.class);
	private Double minX;
	private Double minY;
	private Double maxX;
	private Double maxY;
	private Integer wkid;
	private Integer gridWidth;
	private Integer gridHeight;
	private SpatialReference sr = null;
	private Double width;
	private Double height;
	Integer numRows;
	Integer numCols;
	
	public GenerateGridParamsBolt(Double minx ,Double miny, Double maxx, Double maxy, Integer wkid)
	{
		
		this.minX=minx;
		this.minY=miny;
		this.maxX=maxx;
		this.maxY=maxy;
		this.wkid = wkid;
	}
	@Override
	public void execute(Tuple tuple) {
		Double cellSize = tuple.getDoubleByField("cellSize");
		String procId = tuple.getStringByField("procId");
		double dCols = Math.ceil(width/cellSize);
		double dRows = Math.ceil(height/cellSize);
		numCols = new Integer((int)dCols);
		numRows = new Integer((int)dRows);
		Point origin = AdjustOrigin(minX,minY,dCols, dRows, cellSize);
		Double ox = origin.getX();
		Double oy = origin.getY();
		for(int i = 0; i < this.numCols; ++i)
		{
			for (int j = 0; j < this.numRows; ++j)
			{
				collector.emit(new Values(procId, ox, oy, cellSize, i, j, numCols, numRows, wkid));
			}
		}
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		Point ll = new Point(minX, minY);
		Point lr = new Point (minX, maxY);
		Point ur = new Point(maxX, maxY);
		Point ul = new Point(minX, maxY);
		this.sr = SpatialReference.create(wkid);
		this.width = GeometryEngine.distance(ll, lr, sr);
		this.height = GeometryEngine.distance(ll, ul, sr);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("procId", "ox", "oy", "cellSize", "colIndex", "rowIndex", "numCols", "numRows", "wkid"));

	}
	
	private Point AdjustOrigin(Double ox, Double oy, double cols, double rows,  Double cellSize)
	{
		
		
		Double gridMaxX = ox + (cols * cellSize);
		Double gridMaxY = oy + (rows * cellSize);
		Point gridUR = new Point(gridMaxX, gridMaxY);
		Point origin = new Point(ox, oy);
		Point gridLR = new Point(gridMaxX, oy);
		Double gridWidth = GeometryEngine.distance(origin, gridLR, sr);
		Double gridHeight = GeometryEngine.distance(gridLR, gridUR, sr);
		Double adjustedX = ox - gridWidth/2;
		Double adjustedY = oy - gridHeight/2;
		Point adjustedOrigin = new Point(adjustedX, adjustedY);
		
		return adjustedOrigin;
	}

}
