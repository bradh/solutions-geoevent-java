package com.esri.geoevent.solutions.transport.mlobi;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.params.HttpParams;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.http.GeoEventHttpClient;
import com.esri.ges.core.http.GeoEventHttpClientService;
import com.esri.ges.core.http.KeyValue;
import com.esri.ges.datastore.agsconnection.LayerDetails;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.RestOutboundTransportProvider;
import com.esri.ges.transport.TransportContext;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.transport.http.HttpTransportContext;
import com.esri.ges.transport.http.HttpUtil;
import com.esri.ges.util.Validator;


public class MLOBIOutboundTransport extends OutboundTransportBase implements RestOutboundTransportProvider {
	private enum RequestType {GENERATE_TOKEN, QUERY, UPDATE}
	private Thread 										thread;
	private String 										host;
	private String 										user;
	private String 										pw;
	private GeoEventHttpClientService					httpClientService;
	private HttpTransportContext						context;
	private String 										token;
	private static final BundleLogger					LOGGER= BundleLoggerFactory.getLogger(MLOBIOutboundTransport.class);
	private String 										clientUrl;
	protected String									loginUrl;
	protected String									httpMethod;
	private String										acceptableMimeTypes_client;
	protected String									postBodyType;
	protected String									postBody = "";
	private String										headerParams;
	private String										mode;
	private int 										httpTimeoutValue;
	private Boolean 									setheader = false;
	private String 										featureService;
	private String 										layerIndex;
	private boolean										append;
	private volatile String								trackIDField;
	private final HashMap<String, String>				oidCache = new HashMap<String, String>();
	private final ObjectMapper							mapper = new ObjectMapper();
	private String 										oidQueryParams;
	private final List<String>							insertFeatureList = new ArrayList<String>();
	private final List<String>							updateFeatureList = new ArrayList<String>();
	private final StringBuilder							featureBuffer				= new StringBuilder(1024);
	private boolean										cleanupOldFeatures	= false;
	private int											featureLifeSpan;
	private int											cleanupFrequency;
	private volatile String							    cleanupTimeField;
	private CleanupThread								cleanupThread;
	private volatile int								maxTransactionSize	= 500;
	private JsonNode									features;
	private GeoEventHttpClientService					httpService;
	private String										layerDescriptionForLogs;
	
	public MLOBIOutboundTransport(TransportDefinition definition, GeoEventHttpClientService httpClientService)
			throws ComponentException {
		super(definition);
		this.httpClientService = httpClientService;
	}
	
	//Overridden methods
	
	@Override
	public void afterPropertiesSet()
	{
		setup();
	}
	@Override
	public void beforeConnect(TransportContext context)
	{
		try {
			HttpRequest request = ((HttpTransportContext)context).getHttpRequest();
			if(token == null)
			{
				String password = cryptoService.decrypt(pw);
				generateToken(user, password, (HttpTransportContext)context);
			}
			request.setHeader("Cookie", token);
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		
	}
	
	
	
	@Override
	public synchronized void start()
	{
		switch (getRunningState())
		{
			case STARTING:
			case STARTED:
			case ERROR:
				return;
		}
		setRunningState(RunningState.STARTING);
		setup();
		context = new HttpTransportContext();
		context.setHttpClientService(httpClientService);
		setRunningState(RunningState.STARTED);
	}

	@Override
	public synchronized void stop()
	{
		super.stop();
		LOGGER.debug("OUTBOUND_STOP");
		if (token != null)
		{
			token = null;
		}
	}

	
	
	@Override
	public void onReceive(TransportContext context) {
		super.onReceive(context);
		if (token == null) {
			if (!(context instanceof HttpTransportContext))
				return;

			HttpResponse response = ((HttpTransportContext) context)
					.getHttpResponse();

			Header[] headers = response.getAllHeaders();
			for (Header h : headers) {
				if (h.getName().equals("set-cookie")) {
					token = h.getValue();
				}
			}
		}
	}

	@Override
	public void receive(ByteBuffer bb, String channelid) {
		if (host != null || !host.isEmpty())
		{
			byte[] data = new byte[bb.remaining()];
			bb.get(data);
			doHttp(data);
		}
		
	}

	@Override
	public byte[] processCache(HashMap<String, String[]> arg0) {
		
		return null;
	}
	
	public synchronized void setup()
	{
		host = properties.get("server").getValueAsString();
		user = properties.get("username").getValueAsString();
		pw = properties.get("password").getValueAsString();
		featureService = properties.get("featureservice").getValueAsString();
		layerIndex = properties.get("layerindex").getValueAsString();
		loginUrl = host + "/user/login";
		clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/updateFeatures";
		httpMethod = "POST";
		acceptableMimeTypes_client = "application/json";
		postBodyType = "application/x-www-form-urlencoded";
		headerParams = "";
		mode = "CLIENT";
		httpTimeoutValue = GeoEventHttpClient.DEFAULT_TIMEOUT;
		
	}
	
	
		
	private void doHttp(byte[] data)
	{
		if(token == null)
			setheader = true;
		try (GeoEventHttpClient http = httpClientService.createNewClient())
		{
			HttpRequestBase request = HttpUtil.createHttpRequest(http, clientUrl, httpMethod, "", acceptableMimeTypes_client, postBodyType, data, headerParams, LOGGER);
			if (request != null && request instanceof HttpUriRequest)
			{
				context.setHttpRequest(request);
				this.beforeConnect(context);
				CloseableHttpResponse response;
				try
				{
					response = http.execute(request, httpTimeoutValue);
					if (response != null)
					{
					// check if we were in error state - if so then set state to running
						// - we have reconnected
						if (getRunningState() == RunningState.ERROR)
						{
							LOGGER.info("RECONNECTION_MSG", clientUrl);
							setErrorMessage(null);
							setRunningState(RunningState.STARTED);
						}
						
						context.setHttpResponse(response);
						this.onReceive(context);
					}
					else
					{
						// log only if we were not in error state already
						if (getRunningState() != RunningState.ERROR)
						{
							String errorMsg = LOGGER.translate("FAILED_HTTP_METHOD", clientUrl, httpMethod);
							LOGGER.info(errorMsg);

							// set the error state
							setErrorMessage(errorMsg);
							setRunningState(RunningState.ERROR);
						}
					}
				}
				catch (IOException e)
				{
					// log only if we were not in error state already
					if( getRunningState() != RunningState.ERROR )
					{
						
						String errorMsg = LOGGER.translate("ERROR_ACCESSING_URL", clientUrl, e.getMessage());
						LOGGER.error(errorMsg);
						LOGGER.info(e.getMessage(), e);
						
						// set the error state
						setErrorMessage(errorMsg);
						setRunningState(RunningState.ERROR);
					}
				}
			}
		}
		catch (Exception exp)
		{
			LOGGER.error(exp.getMessage(), exp);
		}
	}

	protected void doHttp()
	{
		try (GeoEventHttpClient http = httpClientService.createNewClient())
		{
			HttpRequestBase request = HttpUtil.createHttpRequest(http, clientUrl, httpMethod, "", acceptableMimeTypes_client, postBodyType, postBody, headerParams, LOGGER);
			if (request != null && request instanceof HttpUriRequest)
			{
				context.setHttpRequest(request);
				this.beforeConnect(context);
				CloseableHttpResponse response = null;
				try
				{
					response = http.execute(request, httpTimeoutValue);
					if (response != null)
					{
						// check if we were in error state - if so then set state to running
						// - we have reconnected
						if (getRunningState() == RunningState.ERROR)
						{
							LOGGER.info("RECONNECTION_MSG", clientUrl);
							setErrorMessage(null);
							setRunningState(RunningState.STARTED);
						}

						context.setHttpResponse(response);
						this.onReceive(context);
					}
					else
					{
						// log only if we were not in error state already
						if (getRunningState() != RunningState.ERROR)
						{
							String errorMsg = LOGGER.translate("RESPONSE_FAILURE", clientUrl);
							LOGGER.info(errorMsg);

							// set the error state
							setErrorMessage(errorMsg);
							setRunningState(RunningState.ERROR);
						}
					}
				}
				catch (IOException e)
				{
					// log only if we were not in error state already
					if( getRunningState() != RunningState.ERROR )
					{
						
						String errorMsg = LOGGER.translate("ERROR_ACCESSING_URL", clientUrl, e.getMessage());
						LOGGER.error(errorMsg);
						LOGGER.info(e.getMessage(), e);
						
						// set the error state
						setErrorMessage(errorMsg);
						setRunningState(RunningState.ERROR);
					}
				}
				finally
				{
					IOUtils.closeQuietly(response);
				}
			}
		}
		catch (Exception exp)
		{
			LOGGER.error(exp.getMessage(), exp);
		}
	}
	private void doHttp(String jsonString, RequestType type)
	{
		try
		{
			if(type == RequestType.GENERATE_TOKEN)
			{
				GeoEventHttpClient http = httpClientService.createNewClient();
				clientUrl = host + "/user/login";
				String requestBody = generateTokenPayLoad();
				HttpRequestBase request = HttpUtil.createHttpRequest(http, clientUrl, "POST", "", "application/json", "application/x-www-form-urlencoded", requestBody, LOGGER);
				doHttp(http, request);
			}
			else if(type == RequestType.QUERY)
			{
				GeoEventHttpClient http = httpClientService.createNewClient();
				clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/query";
				String params = "";
				//String params = constructQueryParams();
				
				HttpRequestBase request = HttpUtil.createHttpRequest(http, clientUrl, "GET", params, acceptableMimeTypes_client, postBodyType,  headerParams, LOGGER);
				
				doHttp(http, request);
			}
			else if(type == RequestType.UPDATE)
			{
				GeoEventHttpClient http = httpClientService.createNewClient();
				clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/updateFeatures";
				try{
					features = mapper.readTree(jsonString);
				}
				catch (Exception ex)
				{
					LOGGER.error("ERROR_SENDING_JSON", jsonString, ex.getMessage());
					LOGGER.debug(ex.getMessage(), ex);
					return;
				}
				if (!features.isArray())
				{
					LOGGER.error("INPUT_IS_NOT_AN_ARRAY");
					return;
				}
				ArrayList<String> missingTrackIDs = getListOfUncachedTrackIDs(features);
				queryForMissingOIDs(missingTrackIDs);
				buildJSONStrings(features);
				String requestBody = "";
				//String requestBody = generateUpdatePayLoad(byte[] data);
				HttpRequestBase request = HttpUtil.createHttpRequest(http, clientUrl, httpMethod, "", acceptableMimeTypes_client, postBodyType, requestBody, headerParams, LOGGER);
				doHttp(http, request);
				
				http = httpClientService.createNewClient();
				clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/addFeatures";
				requestBody = "";
				//String requestBody = generateInsertPayLoad(byte[] data);
				request = HttpUtil.createHttpRequest(http, clientUrl, httpMethod, "", acceptableMimeTypes_client, postBodyType, requestBody, headerParams, LOGGER);
				doHttp(http, request);
			}
			
		}
		catch(Exception e)
		{
			LOGGER.error(e.getMessage(), e);
		}
	}
	
	
	private void doHttp(GeoEventHttpClient http, HttpRequestBase request)
	{
		if (request != null && request instanceof HttpUriRequest)
		{
			context.setHttpRequest(request);
			this.beforeConnect(context);
			CloseableHttpResponse response;
			try
			{
				response = http.execute(request, httpTimeoutValue);
				if (response != null)
				{
				// check if we were in error state - if so then set state to running
					// - we have reconnected
					if (getRunningState() == RunningState.ERROR)
					{
						LOGGER.info("RECONNECTION_MSG", clientUrl);
						setErrorMessage(null);
						setRunningState(RunningState.STARTED);
					}
					
					context.setHttpResponse(response);
					this.onReceive(context);
				}
				else
				{
					// log only if we were not in error state already
					if (getRunningState() != RunningState.ERROR)
					{
						String errorMsg = LOGGER.translate("FAILED_HTTP_METHOD", clientUrl, httpMethod);
						LOGGER.info(errorMsg);

						// set the error state
						setErrorMessage(errorMsg);
						setRunningState(RunningState.ERROR);
					}
				}
			}
			catch (IOException e)
			{
				// log only if we were not in error state already
				if( getRunningState() != RunningState.ERROR )
				{
					
					String errorMsg = LOGGER.translate("ERROR_ACCESSING_URL", clientUrl, e.getMessage());
					LOGGER.error(errorMsg);
					LOGGER.info(e.getMessage(), e);
					
					// set the error state
					setErrorMessage(errorMsg);
					setRunningState(RunningState.ERROR);
				}
			}
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
	
	private void generateToken(String username, String pw, HttpTransportContext context) throws Exception
	{
		try
		{
			String userKey = surroundQuotes("username");
			String userString = surroundQuotes(user);
			String pwKey = surroundQuotes("password");
			String password;

			password = cryptoService.decrypt(pw);

			String passwordString = surroundQuotes(password);
			String content = userKey + ":" + userString + "," + pwKey + ":"
					+ passwordString;
			String requestBody = surroundCurlyBrackets(content);
			String url = host + "/user/login";
			GeoEventHttpClient http = httpClientService.createNewClient();
			HttpRequestBase request = HttpUtil.createHttpRequest(http, url, "POST", "", "application/json", "application/x-www-form-urlencoded", requestBody, LOGGER);
			context.setHttpRequest(request);
			
			CloseableHttpResponse response;
			try
			{
				response = http.execute(request, httpTimeoutValue);
				if(response != null)
				{
					if (getRunningState() == RunningState.ERROR)
					{
						LOGGER.info("RECONNECTION_MSG", url);
						setErrorMessage(null);
						setRunningState(RunningState.STARTED);
					}
					context.setHttpResponse(response);
					this.onReceive(context);
				}
				else
				{
					// log only if we were not in error state already
					if (getRunningState() != RunningState.ERROR)
					{
						String errorMsg = LOGGER.translate("RESPONSE_FAILURE", url);
						LOGGER.info(errorMsg);

						// set the error state
						setErrorMessage(errorMsg);
						setRunningState(RunningState.ERROR);
					}
				}
				
			}
			catch (IOException e)
			{
				
			}
		}
		catch(Exception e)
		{
			throw(e);
		}
		
		
	}
	
	private ArrayList<String> getListOfUncachedTrackIDs(JsonNode features)
	{
		ArrayList<String> missingTrackIDs = new ArrayList<String>();
		for (JsonNode feature : features)
		{
			JsonNode attributes = feature.get("attributes");
			JsonNode trackIDNode = attributes.get(trackIDField);
			if (trackIDNode != null)
			{
				String trackID = getTrackIdAsString(trackIDNode);
				if (!oidCache.containsKey(trackID))
				{
					if (missingTrackIDs == null)
						missingTrackIDs = new ArrayList<String>();
					if (!missingTrackIDs.contains(trackID))
						missingTrackIDs.add(trackID);
				}
			}
		}
		return missingTrackIDs;
	}
	
	private void performTheUpdateOperations(List<String> featureList) throws IOException
	{
		while (featureList.size() > maxTransactionSize)
			performTheUpdateOperations(featureList.subList(0, maxTransactionSize));

		String responseString = performTheUpdate(featureList);
		try
		{
			validateResponse(responseString);
		}
		catch (Exception e1)
		{
			if (responseString == null)
			{
				LOGGER.error("UPDATE_FAILED_NULL_RESPONSE");
			}
			else
			{
				LOGGER.debug("UPDATE_FAILED_WITH_RESPONSE", responseString);
				List<String> updatedFeatureList = cleanStaleOIDsFromOIDCache(featureList);
				responseString = performTheUpdate(updatedFeatureList);
				try
				{
					validateResponse(responseString);
				}
				catch (Exception e2)
				{
					LOGGER.error(responseString);
					LOGGER.error("FS_WRITE_ERROR", featureService, e2.getMessage());
				}
			}
		}
		LOGGER.debug("RESPONSE_HEADER_MSG", responseString);
		if (responseString != null)
		{
			JsonNode response = mapper.readTree(responseString);
			if (response.has("updateResults"))
			{
				for (JsonNode result : response.get("updateResults"))
				{
					if (result.get("success").asBoolean() == false)
					{
						int errorCode = result.get("error").get("code").asInt();
						if (errorCode == 1011 || errorCode == 1019)
						{
							String trackID = moveOIDToInsertList(result.get("objectId").asText(), features);
							LOGGER.debug("UPDATE_FAILED_TRY_INSERT_MSG", errorCode, trackID);
						}
					}
				}
			}
		}
		featureList.clear();
	}

	private String performTheUpdate(List<String> featureList) throws IOException
	{
		clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/updateFeatures";
		URL url = new URL(clientUrl);
		Collection<KeyValue> params = new ArrayList<KeyValue>();
		params.add(new KeyValue("features", makeFeatureListString(featureList)));
		

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("URL_POST_DEBUG", url, paramsToString(params));
		String responseString = postAndGetReply(url, params);
		return responseString;
	}

	private void performTheInsertOperations(List<String> featureList) throws IOException
	{
		while (featureList.size() > maxTransactionSize)
			performTheInsertOperations(featureList.subList(0, maxTransactionSize));

		clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/addFeatures";
		URL url = new URL(clientUrl);
		Collection<KeyValue> params = new ArrayList<KeyValue>();
		params.add(new KeyValue("features", makeFeatureListString(featureList)));

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("URL_POST_DEBUG", url, paramsToString(params));
		String responseString = postAndGetReply(url, params);
		validateResponse(responseString);
		LOGGER.debug("RESPONSE_HEADER_MSG", responseString);
		featureList.clear();
	}

	private String makeFeatureListString(List<String> featureList)
	{
		featureBuffer.setLength(0);
		featureBuffer.append("[");
		for (String feature : featureList)
		{
			featureBuffer.append(feature);
			featureBuffer.append(",");
		}
		featureBuffer.deleteCharAt(featureBuffer.length() - 1);
		featureBuffer.append("]");
		return featureBuffer.toString();
	}

	private String moveOIDToInsertList(String objectId, JsonNode features) throws IOException
	{
		for (JsonNode feature : features)
		{
			JsonNode attributes = feature.get("attributes");
			if (attributes.has(trackIDField) && attributes.has("objectId"))
			{
				String trackID = attributes.get(trackIDField).asText();
				String oid = attributes.get("objectId").asText();
				if (oid != null && oid.equals(objectId))
				{
					((ObjectNode) attributes).remove("objectId");
					if (oidCache.containsKey(trackID))
						oidCache.remove(trackID);
					insertFeatureList.add(mapper.writeValueAsString(feature));
					return trackID;
				}
			}
		}
		return null;
	}
	
	private void buildJSONStrings(JsonNode features) throws NumberFormatException, IOException
	{
		for (JsonNode feature : features)
		{
			/*if (!layerDetails.iszEnabled())
			{
				JsonNode geometry = feature.get("geometry");
				if (geometry != null && geometry.has("z"))
				{
					((ObjectNode) geometry).remove("z");
				}
			}*/
			if (!append)
			{
				JsonNode attributes = feature.get("attributes");
				JsonNode trackIDNode = attributes.get(trackIDField);
				if (trackIDNode == null)
				{
					LOGGER.warn("FAILED_TO_UPDATE_INVALID_TRACK_ID_FIELD", trackIDField);
				}
				else
				{
					// String trackID = trackIDNode.getTextValue();
					String trackID = getTrackIdAsString(trackIDNode);

					if (oidCache.containsKey(trackID))
					{
						String oid = oidCache.get(trackID);
						String newFeatureString = createFeatureWithOID(feature, oid);
						updateFeatureList.add(newFeatureString);
						continue;
					}
				}
			}
			insertFeatureList.add(mapper.writeValueAsString(feature));
		}
	}
	
	private String createFeatureWithOID(JsonNode feature, String oid) throws IOException
	{
		JsonNode attributes = feature.get("attributes");
		String oidField = "objectId";
		if (attributes.has(oidField))
			((ObjectNode) attributes).remove(oidField);
		if (oid != null)
			((ObjectNode) attributes).put(oidField, Integer.parseInt(oid));
		String newFeatureString = mapper.writeValueAsString(feature);
		return newFeatureString;
	}
	
	private void queryForMissingOIDs(List<String> missingTrackIDs) throws IOException
	{
		if (missingTrackIDs.size() == 0)
			return;
		while (missingTrackIDs.size() > maxTransactionSize)
			queryForMissingOIDs(missingTrackIDs.subList(0, maxTransactionSize));

		StringBuffer buf = new StringBuffer(1024);
		for (String trackID : missingTrackIDs)
		{
			LOGGER.debug("QUERYING_FOR_MISSING_TRACK_ID", trackID);
			if (buf.length() == 0)
				buf.append(trackIDField + " IN (");
			else
				buf.append(",");
			buf.append("\'" + trackID + "\'");
		}
		buf.append(")");
		missingTrackIDs.clear();
		String whereString = buf.toString();
		performMissingOIDQuery(whereString);
	}
	
	private void performMissingOIDQuery(String whereString) throws IOException
	{
		Collection<KeyValue> params = new ArrayList<KeyValue>();
		params.add(new KeyValue("where", whereString));
		params.add(new KeyValue("outfields", trackIDField + "," + "objectId"));
		params.add(new KeyValue("returnGeometry", "false"));
		clientUrl = host + "/rest/services/" + featureService + "/" + layerIndex + "/query";
		URL url = new URL(clientUrl);
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("URL_POST_DEBUG", url, paramsToString(params));

		String responseString = postAndGetReply(url, params);
		try
		{
			validateResponse(responseString);
		}
		catch (IOException ex)
		{
			LOGGER.error("URL_POST_ERROR", ex, url, paramsToString(params));
			throw ex;
		}
		LOGGER.debug("RESPONSE_HEADER_MSG", responseString);
		JsonNode response = mapper.readTree(responseString);
		if (!response.has("features"))
			return;
		for (JsonNode feature : response.get("features"))
		{
			JsonNode attributes = feature.get("attributes");
			String oid = String.valueOf(attributes.get("objectId"));
			// String trackID = attributes.get(trackIDField).getTextValue();
			String trackID = getTrackIdAsString(attributes.get(trackIDField));

			if (trackID != null)
			{
				oidCache.put(trackID, oid);
			}
		}
	}
	private String postAndGetReply(URL url, Collection<KeyValue> params) throws IOException
	{
		String responString = null;
		try (GeoEventHttpClient http = httpService.createNewClient())
		{
			HttpPost postRequest = http.createPostRequest(url, params);
			postRequest.addHeader("Cookie", token);
			responString = http.executeAndReturnBody(postRequest, GeoEventHttpClient.DEFAULT_TIMEOUT);
		}
		catch (Exception e)
		{
			LOGGER.debug(e.getMessage());
		}
		return responString;
	}
	private String getTrackIdAsString(JsonNode trackIDNode)
	{
		String output = null;
		if (trackIDNode.isTextual())
			output = trackIDNode.getTextValue();
		else if (trackIDNode.isInt())
			output = Integer.toString(trackIDNode.getIntValue());
		else if (trackIDNode.isLong())
			output = Long.toString(trackIDNode.getLongValue());
		else if (trackIDNode.isDouble())
			output = Double.toString(trackIDNode.getDoubleValue());
		else if (trackIDNode.isFloatingPointNumber())
			output = trackIDNode.getDecimalValue().toString();

		if (!Validator.isEmpty(output))
		{
			output = output.replace("'", "''");
		}
		return output;
	}
	
	private List<String> cleanStaleOIDsFromOIDCache(List<String> featureList) throws JsonProcessingException, IOException
	{

		// Construct a list of oids based on the update list
		ArrayList<String> cachedTrackIDValuesThatMightBeStale = new ArrayList<String>();
		for (String featureString : featureList)
		{
			JsonNode feature = mapper.readTree(featureString);
			JsonNode attributes = feature.get("attributes");
			JsonNode trackIDNode = attributes.get(trackIDField);
			if (trackIDNode != null)
			{
				String trackID = trackIDNode.asText();
				cachedTrackIDValuesThatMightBeStale.add(trackID);
				oidCache.remove(trackID);
			}
		}

		if (cachedTrackIDValuesThatMightBeStale.isEmpty())
			return featureList;

		queryForMissingOIDs(cachedTrackIDValuesThatMightBeStale);

		ArrayList<String> updatedFeatures = new ArrayList<String>();

		for (String featureString : featureList)
		{
			JsonNode feature = mapper.readTree(featureString);
			JsonNode attributes = feature.get("attributes");
			JsonNode trackIDNode = attributes.get(trackIDField);
			if (trackIDNode != null)
			{
				String trackID = trackIDNode.asText();
				if (oidCache.containsKey(trackID))
				{
					updatedFeatures.add(createFeatureWithOID(feature, oidCache.get(trackID)));
				}
				else
				{
					insertFeatureList.add(createFeatureWithOID(feature, null));
				}
			}
		}
		return updatedFeatures;

	}
	
	private void validateResponse(String responseString) throws IOException
	{
		if (responseString == null || mapper.readTree(responseString).has("error"))
			throw new IOException((responseString == null) ? "null response" : responseString);
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
	
	private String paramsToString(Collection<KeyValue> params)
	{
		StringBuilder sb = new StringBuilder();
		for (KeyValue param : params)
		{
			if (sb.length() > 0)
				sb.append('&');
			sb.append(param.getKey());
			sb.append('=');
			sb.append(param.getValue() == null ? "" : param.getValue());
		}
		return sb.toString();
	}
	
	private class CleanupThread extends Thread
	{
		private volatile boolean	running	= true;

		private void dismiss()
		{
			running = false;
		}

		@Override
		public void run()
		{
			while (running)
			{
				try
				{
					//cleanup();
					sleep(cleanupFrequency * 1000);
				}
				catch (InterruptedException ex)
				{
					LOGGER.error("CLEANUP_THREAD_INTERRUPTED", ex.getMessage());
					LOGGER.info(ex.getMessage(), ex);
				}
				catch (Throwable t)
				{
					LOGGER.error("CLEANUP_ERROR", t, cleanupFrequency);
					LOGGER.info(t.getMessage(), t);
				}
			}
		}
	}

}
