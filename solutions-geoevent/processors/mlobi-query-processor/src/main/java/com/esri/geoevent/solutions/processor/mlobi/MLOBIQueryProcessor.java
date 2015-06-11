package com.esri.geoevent.solutions.processor.mlobi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Observable;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.http.GeoEventHttpClient;
import com.esri.ges.core.http.GeoEventHttpClientService;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;
import com.esri.ges.transport.http.HttpTransportContext;
import com.esri.ges.transport.http.HttpUtil;

public class MLOBIQueryProcessor extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable{
	private GeoEventCreator 							geoEventCreator;
	private GeoEventDefinitionManager 					manager;
	private Messaging 									messaging;
	private GeoEventProducer 							geoEventProducer;
	private String 										token;
	private static final BundleLogger					LOGGER = BundleLoggerFactory.getLogger(MLOBIQueryProcessor.class);
	private String										host;
	private String										user;
	private String 										pw;
	private String 										querystring;
	private GeoEventHttpClientService					httpClientService;
	private HttpTransportContext						context;
	
	public MLOBIQueryProcessor(GeoEventProcessorDefinition definition)
			throws ComponentException {
		super(definition);
	}
	
	//Overriden methods
	
	@Override
	public void afterPropertiesSet() {
		host = getProperty("host").getValueAsString();
		user = getProperty("user").getValueAsString();
		pw = getProperty("password").getValueAsString();
		querystring = getProperty("query").getValueAsString();
		
	}
	
	@Override
	public synchronized void validate()
	{
		if (token == null) {
			try {
				connectOBI();
			} catch (ValidationException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}
	
	@Override
	public void onServiceStop()
	{
		try {
			disconnectOBI();
		} catch (HttpException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	@Override
	public GeoEvent process(GeoEvent geoEvt) throws Exception {

		return null;
	}

	@Override
	public void disconnect() {
		if (geoEventProducer != null)
			geoEventProducer.disconnect();
	}

	@Override
	public EventDestination getEventDestination() {
		return null;
	}

	@Override
	public String getStatusDetails() {
		return (geoEventProducer != null) ? geoEventProducer.getStatusDetails()
				: "";
	}

	@Override
	public void init() throws MessagingException {
		;
	}

	@Override
	public boolean isConnected() {
		return (geoEventProducer != null) ? geoEventProducer.isConnected()
				: false;
	}

	@Override
	public void setup() throws MessagingException {
		;
	}

	@Override
	public void update(Observable o, Object arg) {
		;
		
	}

	@Override
	public List<EventDestination> getEventDestinations() {
		return (geoEventProducer != null) ? Arrays.asList(geoEventProducer
				.getEventDestination()) : new ArrayList<EventDestination>();
	}

	@Override
	public void send(GeoEvent geoEvent) throws MessagingException {
		if (geoEventProducer != null && geoEvent != null)
			geoEventProducer.send(geoEvent);
		
	}
	
	//Getters Setters
	public void setManager(GeoEventDefinitionManager manager) {
		this.manager = manager;
	}

	public void setMessaging(Messaging messaging) {
		this.messaging = messaging;
		this.geoEventCreator = messaging.createGeoEventCreator();
	}
	
	//helper methods
	private String surroundQuotes(String in)
	{
		String out = "\"" + in + "\"";
		return out;
	}
	
	private String surroundCurlyBrackets(String in)
	{
		return "{" + in + "}";
	}
	
	private String surroundBrackets(String in)
	{
		return "[" + in + "]";
	}
	
	private void connectOBI() throws ValidationException
	{
		try {
			String cookie = null;
			context = new HttpTransportContext();
			context.setHttpClientService(httpClientService);
			GeoEventHttpClient http = httpClientService.createNewClient();
			String clientUrl = host + "/user/login";
			String requestBody = generateTokenPayLoad();
			HttpRequestBase request = HttpUtil.createHttpRequest(http,
					clientUrl, "POST", "", "application/json",
					"application/x-www-form-urlencoded", requestBody, LOGGER);
			request.setHeader("Cookie", token);
			context.setHttpRequest(request);
			HttpResponse response = ((HttpTransportContext) context)
					.getHttpResponse();

			Header[] headers = response.getAllHeaders();
			for (Header h : headers) {
				if (h.getName().equals("set-cookie")) {
					cookie = h.getValue();
				}
			}
			if (cookie == null)
			{
				throw new ValidationException("Unable to retrieve token");
			}
			else
			{
				token = cookie;
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	private void disconnectOBI() throws HttpException
	{
		GeoEventHttpClient http = httpClientService.createNewClient();
		String logouturl = host + "/user/logout" ;
		HttpRequestBase request = HttpUtil.createHttpRequest(http, logouturl, "POST", "", "application/json", "application/x-www-form-urlencoded", "", LOGGER);
		request.setHeader("Cookie", token);
		context.setHttpRequest(request);
		HttpResponse response = ((HttpTransportContext) context)
				.getHttpResponse();
		if(response.getStatusLine().getStatusCode()!=299)
		{
			throw new HttpException("Http Error " + ((Integer)response.getStatusLine().getStatusCode()).toString() + " " + response.getStatusLine().getReasonPhrase());
		}

	}
	
	private String generateTokenPayLoad() throws Exception
	{
		try {
			String userKey = surroundQuotes("username");
			String userString = surroundQuotes(user);
			String pwKey = surroundQuotes("password");
			String password;
			password = cryptoService.decrypt(pw);
			String passwordString = surroundQuotes(password);
			String content = userKey + ":" + userString + "," + pwKey + ":"
					+ passwordString;
			String requestBody = surroundCurlyBrackets(content);
			return requestBody;
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			throw (e);
		}
	}

}
