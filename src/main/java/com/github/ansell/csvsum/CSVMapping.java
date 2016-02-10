/**
 * 
 */
package com.github.ansell.csvsum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * A mapping definition from an original CSV field to an output CSV field.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
class CSVMapping implements Function<String, String> {

	enum CSVMappingLanguage {
		JAVASCRIPT
	}

	private CSVMappingLanguage language;
	private String input;
	private String output;
	private String mapping = "input";
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
			Map<String, List<CSVMapping>> map) {

		Map<String, String> outputValues = new HashMap<>();
		List<String> result = new ArrayList<>();

		for (int i = 0; i < inputHeaders.size(); i++) {
			String originalHeader = inputHeaders.get(i);
			String originalValue = line.get(i);

			List<CSVMapping> nextMap = map.get(originalHeader);

			for (CSVMapping nextMapping : nextMap) {
				outputValues.put(nextMapping.getOutputField(), nextMapping.apply(originalValue));
			}
		}

		// Then order them consistenty with the list of output headers
		for (String nextOutputHeader : outputHeaders) {
			if (!outputValues.containsKey(nextOutputHeader)) {
				throw new RuntimeException("Mapped value was not found for output field: " + nextOutputHeader);
			}

			result.add(outputValues.get(nextOutputHeader));
		}

		return result;
	}

	@Override
	public String apply(String input) {

		if (this.language != CSVMappingLanguage.JAVASCRIPT) {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}

		// evaluate JavaScript code and access the variable that results from
		// the mapping
		try {
			ScriptEngineManager manager = new ScriptEngineManager();
			ScriptEngine engine = manager.getEngineByName("nashorn");

			engine.put("input", input);

			return (String) engine.eval("return " + this.mapping + ";");
			// return (String) engine.get("output");
		} catch (ScriptException e) {
			throw new RuntimeException(e);
		}
	}
}
