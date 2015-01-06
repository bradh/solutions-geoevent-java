package com.esri.geoevent.solutions.processor.hotspot;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;
import com.esri.ges.core.geoevent.FieldType;

public class GeoEventTupleHelper {
	private static final Log LOG = LogFactory.getLog(GeoEventTupleHelper.class);
	public static String GetFieldTypeString(FieldType t) throws IOException
	{
		String type=null;
		if(t==FieldType.Boolean)
		{
			type="boolean";
		}
		else if(t==FieldType.Date)
		{
			type="date";
		}
		else if(t==FieldType.Double)
		{
			type="double";
		}
		else if(t==FieldType.Float)
		{
			type="float";
		}
		else if(t==FieldType.Geometry)
		{
			type="geometry";
		}
		else if(t==FieldType.Group)
		{
			type="group";
		}
		else if(t==FieldType.Integer)
		{
			type="integer";
		}
		else if(t==FieldType.Long)
		{
			type="long";
		}
		else if(t==FieldType.Short)
		{
			type="short";
		}
		else if(t==FieldType.String)
		{
			type="string";
		}
		else
		{
			IOException e = new IOException("Invalid input");
			LOG.error(e.getMessage());
			throw(e);
		}
		return type;
	}
	
	public static String GetGeoTypeString(Geometry.Type t) throws IOException
	{
		
		String type = null;
		if(t == Type.MultiPoint)
		{
			type = "multipoint";
		}
		else if(t == Type.Point)
		{
			type = "point";
		}
		else if(t == Type.Polyline)
		{
			type = "polyline";
		}
		else if(t == Type.Polygon)
		{
			type = "polygon";
		}
		else if(t == Type.Envelope)
		{
			type = "envelope";
		}
		else if(t == Type.Line)
		{
			type = "Line";
		}
		else if(t == Type.Unknown)
		{
			type = "unknown";
		}
		
		else
		{
			IOException e = new IOException("Invalid input");
			LOG.error(e.getMessage());
			throw(e);
		}
		return type;
	}
	
	public static FieldType GetFieldTypeFromString(String t) throws IOException
	{
		FieldType type=null;
		if(t.equals("boolean"))
		{
			type=FieldType.Boolean;
		}
		else if(t.equals("date"))
		{
			type=FieldType.Date;
		}
		else if(t.equals("double"))
		{
			type=FieldType.Double;
		}
		else if(t.equals("float"))
		{
			type=FieldType.Float;
		}
		else if(t.equals("geometry"))
		{
			type=FieldType.Geometry;
		}
		else if(t.equals("group"))
		{
			type=FieldType.Group;
		}
		else if(t.equals("integer"))
		{
			type=FieldType.Integer;
		}
		else if(t.equals("long"))
		{
			type=FieldType.Long;
		}
		else if(t.equals("short"))
		{
			type=FieldType.Short;
		}
		else if(t.equals("string"))
		{
			type=FieldType.String;
		}
		else
		{
			IOException e = new IOException("Invalid input");
			LOG.error(e.getMessage());
			throw(e);
		}
		return type;
		
	}
	
	public static Geometry.Type GetGeoTypeFromString(String t) throws IOException
	{
		
		Geometry.Type type = null;
		if(t.equals("multipoint") )
		{
			type =Type.MultiPoint;
		}
		else if(t.equals("point")) 
		{
			type = Type.Point;
		}
		else if(t.equals("polyline"))
		{
			type = Type.Polyline;
		}
		else if(t.equals("polygon"))
		{
			type = Type.Polygon;
		}
		else if(t.equals("envelope") )
		{
			type = Type.Envelope;
		}
		else if(t.equals("line"))
		{
			type = Type.Line;
		}
		else if(t.equals("unknown"))
		{
			type = Type.Unknown;
		}
		else
		{
			IOException e = new IOException("Invalid input");
			LOG.error(e.getMessage());
			throw(e);
		}
		return type;

	}
}
