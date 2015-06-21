package com.esri.geoevent.solutions.processor.mlobi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.esri.ges.core.condition.ConditionDefinitionBase;
import com.esri.ges.core.property.LabeledValue;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;


public class MLOBIQueryConditionDefinition extends ConditionDefinitionBase {
	private static final BundleLogger					LOGGER= BundleLoggerFactory.getLogger(MLOBIQueryConditionDefinition.class);
	
	
	public MLOBIQueryConditionDefinition(){
		try
		{
			List<LabeledValue> mlobiOperatorValues = new ArrayList<LabeledValue>();
			mlobiOperatorValues.add(new LabeledValue("${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LV_OBI_QUERY_AND}", "AND"));
			mlobiOperatorValues.add(new LabeledValue("${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LV_OBI_QUERY_OR}", "OR"));
			mlobiOperatorValues.add(new LabeledValue("${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LV_OBI_QUERY_NOT_IN}", "NOT_IN"));
			mlobiOperatorValues.add(new LabeledValue("${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LV_OBI_QUERY_NEAR}", "NEAR"));
			mlobiOperatorValues.add(new LabeledValue("${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.LV_OBI_QUERY_NEAR_N}", "NEAR_N"));
		
			propertyDefinitions.put("operand",  new PropertyDefinition("operand", PropertyType.String, null, "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.CONDITION_OPERAND_LBL}", "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.CONDITION_OPERAND_DESC}", true, false));
		    propertyDefinitions.put("operator", new PropertyDefinition("operator", PropertyType.Undefined, null, "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.CONDITION_OPERATOR_LBL}", "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.CONDITION_OPERATOR_DESC}", true, false, mlobiOperatorValues));
		    propertyDefinitions.put("value",    new PropertyDefinition("value", PropertyType.Undefined, null, "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.CONDITION_VALUE_LBL}", "${com.esri.geoevent.solutions.processor.mlobi.mlobi-query-processor.CONDITION_VALUE_DESC}", "operator=AND,operator=OR,operator=NOT_IN,operator=NEAR,operator=NEAR_N", true, false));
		}
		catch(Exception e)
	    {
	      ;
	    }
		
	}
	@Override
	public String getName()
	{
		return "mlobiQueryCondition";
	}
}
