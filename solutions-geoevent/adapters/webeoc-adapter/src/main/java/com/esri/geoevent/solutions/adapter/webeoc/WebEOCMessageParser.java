package com.esri.geoevent.solutions.adapter.webeoc;

import java.util.HashMap;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.esri.ges.core.geoevent.FieldDefinition;

public class WebEOCMessageParser extends DefaultHandler {

	//private static final String MESSAGES_TAG1  = "messages";
	//private static final String MESSAGE_TAG1   = "message";
	private static final String DATA_TAG  = "data";
	private static final String RESULT_TAG   = "result";
	
	private enum MessageLevel
	{
		root, inMessages, inMessage, inAttribute;
	}

	private MessageLevel messageLevel = MessageLevel.root;

	private String attributeName;
	private String attribute;
	private HashMap<String,String> attributes = new HashMap<String,String>();
	private WebEOCInboundAdapter adapter; 
	
	public WebEOCMessageParser(WebEOCInboundAdapter adapter) 
	{
		super();
		this.adapter = adapter;
	}
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException
	{
	
		if(qName == null)
			return;

		if (messageLevel == MessageLevel.root && (qName.equalsIgnoreCase(DATA_TAG)))
		{
			messageLevel = MessageLevel.inMessages;
		}
		else if(messageLevel == MessageLevel.inMessages && (qName.equalsIgnoreCase(RESULT_TAG)))
		{
			messageLevel = MessageLevel.inMessage;
		}
		else if(messageLevel == MessageLevel.inMessage)
		{
			messageLevel = MessageLevel.inAttribute;
			attribute = "";
			attributeName = qName;
		}
		else if(messageLevel == MessageLevel.inAttribute)
		{
			throw new SAXException("Problem parsing message, cannot handle nested attributes. ("+qName+" inside "+attributeName+")");
		}
	}
	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException
	{
		if (messageLevel == MessageLevel.inMessages && (qName.equalsIgnoreCase(DATA_TAG)))
		{
			messageLevel = MessageLevel.root;
		}
		else if (messageLevel == MessageLevel.inMessage && (qName.equalsIgnoreCase(RESULT_TAG)))
		{
			messageLevel = MessageLevel.inMessages;
			adapter.queueGeoEvent(attributes);
			attributes.clear();
		}
		else if (messageLevel == MessageLevel.inAttribute && qName.equalsIgnoreCase(attributeName))
		{
			messageLevel = MessageLevel.inMessage;
			attributes.put( attributeName, attribute );
			attributeName = null;
		}

	}

	@Override
	public void characters(char ch[], int start, int length) throws SAXException
	{
		if (messageLevel == MessageLevel.inAttribute)
		{
			attribute = new String(ch, start, length);
		}
	}

	public void setAdapter( WebEOCInboundAdapter adapter )
	{
		this.adapter = adapter;
	}
}
