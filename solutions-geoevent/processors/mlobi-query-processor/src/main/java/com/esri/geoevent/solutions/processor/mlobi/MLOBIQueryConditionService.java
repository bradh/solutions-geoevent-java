package com.esri.geoevent.solutions.processor.mlobi;

import org.osgi.framework.BundleContext;

import com.esri.ges.condition.ConditionService;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.condition.Condition;
import com.esri.ges.core.condition.ConditionDefinition;

public class MLOBIQueryConditionService implements ConditionService{

	MLOBIQueryConditionDefinition definition;
	BundleContext context;
	public MLOBIQueryConditionService()
	{
		definition = new MLOBIQueryConditionDefinition();
	}
	@Override
	public Condition create() throws ComponentException {
		return new MLOBIQueryCondition(definition);
	}

	@Override
	public ConditionDefinition getDefinition() {
		return definition;
	}

	@Override
	public void setBundleContext(BundleContext context) {
		definition.setBundleContext(context);
		
	}

}
