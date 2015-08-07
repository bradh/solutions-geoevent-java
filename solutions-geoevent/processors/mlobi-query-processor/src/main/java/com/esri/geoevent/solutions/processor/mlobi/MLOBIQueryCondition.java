package com.esri.geoevent.solutions.processor.mlobi;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.esri.ges.condition.ConditionException;
import com.esri.ges.core.component.ComponentBase;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.condition.Condition;
import com.esri.ges.core.condition.ConditionDefinition;
import com.esri.ges.core.geoevent.Field;
import com.esri.ges.core.geoevent.FieldCardinality;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.FieldExpressionTerm;
import com.esri.ges.core.geoevent.FieldGroup;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.operator.LogicalOperator;
import com.esri.ges.core.property.Property;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.validation.Validatable;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.util.Converter;
import com.esri.ges.util.Validator;

public class MLOBIQueryCondition extends ComponentBase implements Condition {
	private static final BundleLogger LOGGER = BundleLoggerFactory
			.getLogger(MLOBIQueryCondition.class);

	private Object operand;
	private String operandParsingError = "";
	private List<Object> values = new ArrayList<Object>();
	private List<String> valuesParsingErrors = new ArrayList<String>();

	public MLOBIQueryCondition(ConditionDefinition definition)
			throws ComponentException {
		super(definition);
	}

	public String getOperand() {
		return hasProperty("operand") ? getProperty("operand")
				.getValueAsString() : "";
	}

	public void setOperand(String operand) throws PropertyException {
		setProperty(new Property(definition.getPropertyDefinition("operand"),
				operand));
	}

	public MLOBIQueryOperator getOperator() {
		return hasProperty("operator") ? (MLOBIQueryOperator) getProperty(
				"operator").getValue() : null;
	}

	public void setOperator(MLOBIQueryOperator operator)
			throws PropertyException {
		if (operator != null)
			setProperty(new Property(
					definition.getPropertyDefinition("operator"), operator));
	}

	public Object getValue() {
		return hasProperty("value") ? getProperty("value").getValue() : null;
	}

	public void setValue(Object value) throws PropertyException {
		if (value != null)
			setProperty(new Property(definition.getPropertyDefinition("value"),
					value));
	}

	@Override
	public void afterPropertySet(String propName) {
		if (propName.equals("operand")) {
			this.operand = null;
			operandParsingError = "";
			try {
				this.operand = parse(getOperand());
			} catch (ValidationException e) {
				operandParsingError = LOGGER.translate("OPERATOR_PARSE_ERROR",
						getOperand(), e.getMessage());
				LOGGER.error(operandParsingError);
			}
		} else
			afterPropertiesSet();
	}

	@Override
	public void afterPropertiesSet() {
		values.clear();
		valuesParsingErrors.clear();
		if (getValue() != null) {
			String s = getValue().toString();
			switch (getOperator()) {
			case ISNULL: 
				break;
			case NOT_IN:
				break;
			case NEAR:
				break;
			case NEAR_IN:
				break;
			default: {
				try {
					values.add(parse(s));
				} catch (ValidationException e) {
					values.add(s);
				}
			}
			}
		}
	}

	private Object parse(String s) throws ValidationException {
		Object token = null;
		List<String> errorMessages = new ArrayList<String>();
		String sToken = Validator.compactWhiteSpaces(s);
		try {
			token = FieldExpression.parse(sToken);
		} catch (ValidationException e) {
			errorMessages.add(e.getMessage());
		}
		if (token == null) {
			try {
				token = GeoEventPropertyName.parse(sToken);
			} catch (ValidationException e) {
				errorMessages.add(e.getMessage());
			}
		}
		if (token == null) {
			StringBuffer sb = new StringBuffer();
			for (String message : errorMessages)
				sb.append("\n").append(message);
			throw new ValidationException(sb.toString());
		}
		return token;
	}

	@Override
	public boolean evaluate(GeoEvent arg0) {
		// TODO Auto-generated method stub
		return false;
	}
}