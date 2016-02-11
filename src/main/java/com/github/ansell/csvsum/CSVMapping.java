/**
 * 
 */
package com.github.ansell.csvsum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
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
	protected static final String DEFAULT_MAPPING = "inputValue";

	enum CSVMappingLanguage {
		DEFAULT, JAVASCRIPT, GROOVY, LUA
	}

	private CSVMappingLanguage language;
	private String input;
	private String output;
	private String mapping = DEFAULT_MAPPING;
	protected static final String LANGUAGE = "Language";
	protected static final String OLD_FIELD = "OldField";
	protected static final String NEW_FIELD = "NewField";
	protected static final String MAPPING = "Mapping";

	private static final ScriptEngineManager SCRIPT_MANAGER = new ScriptEngineManager();

	// Uncomment the following to debug which script engines are available on
	// the classpath
	static {
		List<ScriptEngineFactory> factories = SCRIPT_MANAGER.getEngineFactories();

		System.out.println("Installed script engines:");

		for (ScriptEngineFactory nextFactory : factories) {
			System.out.println(nextFactory.getEngineName());
		}
	}

	private ScriptEngine scriptEngine;

	/**
	 * All creation of CSVMapping objects must be done through the
	 * {@link #newMapping(String, String, String, String)} method.
	 */
	private CSVMapping() {
	}

	static final CSVMapping newMapping(String language, String input, String output, String mapping) {
		CSVMapping result = new CSVMapping();

		try {
			result.language = CSVMappingLanguage.valueOf(language.toUpperCase());
		} catch (IllegalArgumentException e) {
			result.language = CSVMappingLanguage.DEFAULT;
		}

		if (result.language == null) {
			throw new IllegalArgumentException("No mapping language found for: " + language);
		}

		result.input = input;
		result.output = output;
		// By default empty mappings do not change the input, and are
		// efficiently dealt with as such
		if (!mapping.isEmpty()) {
			result.mapping = mapping;
		}

		result.init();

		return result;
	}

	private void init() {
		// Short circuit if the mapping is the default mapping and avoid
		// creating an instance of nashorn for this mapping
		if (this.mapping.equalsIgnoreCase(DEFAULT_MAPPING)) {
			return;
		}

		// precompile the function for this mapping for efficiency
		if (this.language == CSVMappingLanguage.JAVASCRIPT) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("javascript");

				scriptEngine
						.eval("var mapFunction = function(inputHeaders, inputField, inputValue, outputField, line) { return "
								+ this.mapping + "; };");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == CSVMappingLanguage.GROOVY) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("groovy");

				scriptEngine.eval("def mapFunction(inputHeaders, inputField, inputValue, outputField, line) {  "
						+ this.mapping + " }");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == CSVMappingLanguage.LUA) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("lua");

				scriptEngine
						.eval("mapFunction = function(inputHeaders, inputField, inputValue, outputField, line) return  "
								+ this.mapping + " end");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
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

		if (outputHeaders.size() != map.size()) {
			throw new IllegalArgumentException("The number of mappings must match the number of output headers");
		}

		Map<String, String> outputValues = new ConcurrentHashMap<>();
		List<String> result = new ArrayList<>();

		// Note, empirically, it seems about 50% faster with a limited number of
		// cores to do a serial mapping, not a parallel mapping
		// map.parallelStream().forEach(nextMapping -> {
		for (CSVMapping nextMapping : map) {
			outputValues.put(nextMapping.getOutputField(), nextMapping.apply(inputHeaders, line));
		}

		for (String nextOutput : outputHeaders) {
			result.add(outputValues.get(nextOutput));
		}

		return result;
	}

	public String apply(List<String> inputHeaders, List<String> line) {
		String nextInputValue = line.get(inputHeaders.indexOf(getInputField()));

		// Short circuit if the mapping is the default mapping
		if (this.mapping.equalsIgnoreCase(DEFAULT_MAPPING)) {
			return nextInputValue;
		}

		if (this.language == CSVMappingLanguage.JAVASCRIPT || this.language == CSVMappingLanguage.GROOVY
				|| this.language == CSVMappingLanguage.LUA) {
			// evaluate script code and access the variable that results
			// from the mapping
			try {
				return (String) ((Invocable) scriptEngine).invokeFunction("mapFunction", inputHeaders,
						this.getInputField(), nextInputValue, this.getOutputField(), line);
			} catch (ScriptException | NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
	}
}
