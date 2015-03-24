package com.esri.geoevent.solutions.transport.webeoc;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esi911.webeoc7.api._1.ArrayOfString;
import com.esi911.webeoc7.api._1.ArrayOfWebEOCUser;
import com.esi911.webeoc7.api._1.WebEOCCredentials;
import com.esi911.webeoc7.api._1.WebEOCUser;
import com.esi911.webeoc7.api._1_0.API;
import com.esi911.webeoc7.api._1_0.APISoap;

//import com.esri.geoevent.solutions.webeoc7.esi911.com.APISoap;
//import com.esri.geoevent.solutions.webeoc7.esi911.com.WebEOCCredentials;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportContext;
import com.esri.ges.transport.TransportDefinition;

public class WebEOCInboundTransport extends InboundTransportBase implements
		Runnable {
	private static final Log LOG = LogFactory
			.getLog(WebEOCInboundTransport.class);
	private Thread thread = null;
	private String username = null;
	private String pw = null;
	private String webeocUrl = null;
	private String incident = null;
	private String board = null;
	private String position = null;
	private String view = null;
	private Integer pollingInterval = null;
	private APISoap apiSoap = null;
	private long lastTime = -1L;
	String channelId = null;
	WebEOCCredentials credentials = null;

	public WebEOCInboundTransport(TransportDefinition definition)
			throws ComponentException {
		super(definition);

	}

	@SuppressWarnings("incomplete-switch")
	@Override
	public synchronized void start() {
		RunningState state = getRunningState();
		switch (state) {
		case STARTING:
		case STARTED:
		case STOPPING:
		case ERROR:
			return;
		}

		try {

			setRunningState(RunningState.STARTING);
			thread = new Thread(this);
			thread.start();
		} catch (Exception e) {
			LOG.error(e.getStackTrace());
			LOG.error(e.getMessage());
		}
	}

	@Override
	public synchronized void stop() {
		 super.stop();
		 setRunningState(RunningState.STOPPED);
	}

	@Override
	public synchronized void afterPropertiesSet() {
		super.afterPropertiesSet();
		webeocUrl = getProperty("webeocurl").getValueAsString();
		username = getProperty("user").getValueAsString();
		pw = getProperty("password").getValueAsString();
		incident = getProperty("incident").getValueAsString();
		position = getProperty("position").getValueAsString();
		board = getProperty("board").getValueAsString();
		view = getProperty("view").getValueAsString();
		pollingInterval = (Integer) getProperty("pollingInterval").getValue();

	}

	@Override
	public synchronized void validate() throws ValidationException {
		API api = null;
		URL soapEndpoint = null;
		try {
			soapEndpoint = new URL(webeocUrl);
			api = new API(soapEndpoint);
			apiSoap = api.getAPISoap();
			credentials = new WebEOCCredentials();
			credentials.setUsername(username);
			credentials.setPassword(pw);

		} catch (MalformedURLException e) {
			LOG.error(e.getStackTrace());
			LOG.error(e.getMessage());
		}
		try {
			
			ArrayOfString positionArray = apiSoap.getUserPositions(credentials);
			List<String> positions = positionArray.getString();
			if (!positions.contains(position)) {
				ValidationException e = new ValidationException(
						"Cannot locate Position, " + position + " for user, "
								+ username);
				LOG.error(e.getMessage());
				setRunningState(RunningState.ERROR);
				throw (e);
			}
			credentials.setPosition(position);
			ArrayOfWebEOCUser userArray = apiSoap.getUsersByPosition(credentials, position);
			List<WebEOCUser> users = userArray.getWebEOCUser();
			for (WebEOCUser u: users)
			{
				if(u.getUsername().equals(username))
				{
					if(!u.isIsAdministrator())
					{
						ValidationException e = new ValidationException(
								"user must have admin privileges");
						LOG.error(e.getMessage());
						setRunningState(RunningState.ERROR);
						throw (e);
					}
				}
			}
			ArrayOfString incidentArray = apiSoap.getIncidents(credentials);
			List<String> incidents = incidentArray.getString();
			if (!incidents.contains(incident)) {
				ValidationException e = new ValidationException(
						"Cannot locate Incident, " + incident + " for user, "
								+ username);
				LOG.error(e.getMessage());
				setRunningState(RunningState.ERROR);
				throw (e);
			}
			credentials.setIncident(incident);
			/*ArrayOfString boardArray = apiSoap.getBoardNames(credentials);
			List<String> boards = boardArray.getString();
			if (!boards.contains(board)) {
				ValidationException e = new ValidationException(
						"Cannot locate Board, " + board + " for user, "
								+ username);
				LOG.error(e.getMessage());
				throw (e);
			}
			ArrayOfString viewArray = apiSoap.getInputViews(credentials, board);
			List<String> views = viewArray.getString();
			if (!views.contains(view)) {
				ValidationException e = new ValidationException(
						"Cannot locate Voard, " + view + " for user, "
								+ username);
				LOG.error(e.getMessage());
				throw (e);
			}*/
		} catch (Exception e) {
			LOG.error(e.getMessage());
			ValidationException ve = new ValidationException(
					"Error validating properties");
			LOG.error(e.getMessage());
			setRunningState(RunningState.ERROR);
			throw (ve);
		}
	}

	@Override
	public void onReceive(TransportContext context) {
		channelId = UUID.randomUUID().toString();
	}

	@Override
	public void run() {
		setRunningState(RunningState.STARTED);
		while (isRunning()) {
			String results = null;

			long timestamp = System.currentTimeMillis();
			if (lastTime < 0) {
				try {

					results = apiSoap.getData(credentials, board, view);
					if (results != null) {
						byte[] byteArray = results.getBytes();
						receive(byteArray);
					}
					lastTime = timestamp;

				} catch (Exception e) {
					LOG.error(e.getStackTrace());
					LOG.error(e.getMessage());
				}
			} else {
				if (timestamp - lastTime >= pollingInterval) {
					try {
						GregorianCalendar greg = new GregorianCalendar();
						greg.setTimeInMillis(lastTime);
						XMLGregorianCalendar c = DatatypeFactory.newInstance()
								.newXMLGregorianCalendar(greg);

						results = apiSoap.getUpdatedData(credentials, board,
								view, c);
						if (results != null) {
							byte[] byteArray = results.getBytes();
							receive(byteArray);
						}
						lastTime = timestamp;
					} catch (Exception e) {

					}
				}
			}

		}
	}

	private void receive(byte[] bytes) {
		if (bytes != null && bytes.length > 0) {
			ByteBuffer bb = ByteBuffer.allocate(bytes.length);
			bb.put(bytes);
			bb.flip();
			byteListener.receive(bb, channelId);
			bb.clear();
		}
	}
}
