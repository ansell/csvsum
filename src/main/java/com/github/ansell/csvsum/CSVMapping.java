/**
 * 
 */
package com.github.ansell.csvsum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * A mapping definition from an original CSV field to an output CSV field.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
class CSVMapping {

	/**
	 * The default mapping if none is specified in the mapping file.
	 */
	protected static final String DEFAULT_MAPPING = "input";

	enum CSVMappingLanguage {
		JAVASCRIPT
	}

	private CSVMappingLanguage language;
	private String input;
	private String output;
	private String mapping = DEFAULT_MAPPING;
	protected static final String LANGUAGE = "Language";
	protected static final String OLD_FIELD = "OldField";
	protected static final String NEW_FIELD = "NewField";
	protected static final String MAPPING = "Mapping";

	static final CSVMapping getMapping(String language, String input, String output, String mapping) {
		CSVMapping result = new CSVMapping();

		result.language = CSVMappingLanguage.valueOf(language.toUpperCase());
		if (result.language == null) {
			throw new IllegalArgumentException("No mapping language found for: " + language);
		}

		result.input = input;
		result.output = output;
		if (!mapping.isEmpty()) {
			result.mapping = mapping;
		}

		return result;
	}

	CSVMappingLanguage getLanguage() {
		return this.language;
	}

	String getInputField() {
		return this.input;
	}

	String getOutputField() {
		return this.output;
	}

	String getMapping() {
		return this.mapping;
	}

	public static List<String> mapLine(List<String> inputHeaders, List<String> outputHeaders, List<String> line,
			List<CSVMapping> map) {

		Map<String, String> outputValues = new HashMap<>();
		List<String> result = new ArrayList<>();

		for (CSVMapping nextMapping : map) {
			outputValues.put(nextMapping.getOutputField(), nextMapping.apply(inputHeaders, line));
		}

		for(String nextOutput : outputHeaders) {
			result.add(outputValues.get(nextOutput));
		}
		
		return result;
	}

	public String apply(List<String> inputHeaders, List<String> line) {

		if (this.language != CSVMappingLanguage.JAVASCRIPT) {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}

		String nextInputValue = line.get(inputHeaders.indexOf(getInputField()));
		
		// Short circuit if the mapping is the default mapping
		if (this.mapping.equalsIgnoreCase(DEFAULT_MAPPING)) {
			return nextInputValue;
		}

		// evaluate JavaScript code and access the variable that results from
		// the mapping
		try {
			ScriptEngineManager manager = new ScriptEngineManager();
			ScriptEngine engine = manager.getEngineByName("nashorn");

			engine.eval(
					"var mapFunction = function(inputHeaders, inputField, inputValue, outputField, line) { return "
							+ this.mapping + "; };");

			Invocable invocable = (Invocable) engine;

			return (String) invocable.invokeFunction("mapFunction", inputHeaders, this.getInputField(), nextInputValue, this.getOutputField(), line);
			// return (String) engine.get("output");
		} catch (ScriptException | NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}
}
