package com.esri.geoevent.solutions.processor.mlobi;

import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;

public enum MLOBIQueryOperator {
	AND, OR, NOT_IN, NEAR, NEAR_IN;
	
	private static final BundleLogger					LOGGER = BundleLoggerFactory.getLogger(MLOBIQueryOperator.class);
	
	@Override
	public String toString()
	{
		switch(this)
		{
		case AND:
			return "AND";
		case OR:
			return "OR";
		case NOT_IN:
			return "NOT_IN";
		case NEAR:
			return "NEAR";
		case NEAR_IN:
			return "NEAR/";
		default:
			return "";
		}
	}
	
}
