package com.esri.geoevent.solutions.processor.mlobi;

import com.esri.ges.core.component.ComponentBase;
import com.esri.ges.core.component.ComponentDefinition;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.condition.Condition;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.property.Property;
import com.esri.ges.core.property.PropertyException;

public class MLOBIQueryCondition extends ComponentBase implements Condition{

	public MLOBIQueryCondition(ComponentDefinition definition)
			throws ComponentException {
		super(definition);
	}

	@Override
	public boolean evaluate(GeoEvent arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	
	//getters setters
	public String getOperand()
	{
		return hasProperty("operand") ? getProperty("operand").getValueAsString() : "";
	}
	public void setOperand(String operand) throws PropertyException
	{
	  setProperty(new Property(definition.getPropertyDefinition("operand"), operand));
	}

}
