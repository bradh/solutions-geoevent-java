package com.esri.geoevent.solutions.processor.hotspot;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GenerateCellGeometryBolt extends BaseRichBolt {
	private OutputCollector collector;
	private static final Log LOG = LogFactory.getLog(GenerateCellGeometryBolt.class);
	@Override
	public void execute(Tuple tuple) {
		String procId = tuple.getStringByField("procId");
		Double ox = tuple.getDoubleByField("ox");
		Double oy = tuple.getDoubleByField("oy");
		Double cellSize = tuple.getDoubleByField("cellSize");
		Integer colIndex = tuple.getIntegerByField("colIndex");
		Integer rowIndex = tuple.getIntegerByField("rowIndex");
		Integer numCols = tuple.getIntegerByField("numCols");
		Integer numRows = tuple.getIntegerByField("numRows");
		Integer wkid = tuple.getIntegerByField("wkid");
		Double minX = ox + (colIndex*cellSize);
		Double minY = oy + (rowIndex*cellSize);
		Double maxX = ox + ((colIndex+1)*cellSize);
		Double maxY = oy + ((rowIndex+1)*cellSize);
		Point ll = new Point(minX, minY);
		Point lr = new Point(maxX, minY);
		Point ur = new Point(maxX, maxY);
		Point ul = new Point(minX, maxY);
		Polygon cell = new Polygon();
		cell.startPath(ll);
		cell.lineTo(lr);
		cell.lineTo(ur);
		cell.lineTo(ul);
		cell.lineTo(ll);
		cell.closeAllPaths();
		String json = GeometryEngine.geometryToJson(wkid, cell);
		collector.emit(new Values(numCols, numRows, cell));
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("numCols", "numRows", "cell"));

	}

}
