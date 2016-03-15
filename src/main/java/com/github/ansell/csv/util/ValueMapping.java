/*
 * Copyright (c) 2016, Peter Ansell
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.ansell.csv.util;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
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
public class ValueMapping {

	private static final String NO = "no";

	public enum ValueMappingLanguage {
		DEFAULT(ValueMapping.DEFAULT_MAPPING),

		JAVASCRIPT("return inputValue;"),

		GROOVY("inputValue"),

		LUA("return inputValue"),

		ACCESS(""),

		CSVMERGE("");

		private final String defaultMapping;

		ValueMappingLanguage(String defaultMapping) {
			this.defaultMapping = defaultMapping;
		}

		public String getDefaultMapping() {
			return this.defaultMapping;
		}

		public boolean matchesDefaultMapping(String mapping) {
			return this.getDefaultMapping().equals(mapping);
		}
	}

	/**
	 * The default mapping if none is specified in the mapping file.
	 */
	protected static final String DEFAULT_MAPPING = "inputValue";

	public static final String OLD_FIELD = "OldField";
	public static final String NEW_FIELD = "NewField";
	public static final String SHOWN = "Shown";
	public static final String LANGUAGE = "Language";
	public static final String MAPPING = "Mapping";
	private static final ScriptEngineManager SCRIPT_MANAGER = new ScriptEngineManager();
	private static final boolean DEBUG = false;

	static {
		if (DEBUG) {
			System.out.println("Installed script engines:");
			SCRIPT_MANAGER.getEngineFactories().stream().map(ScriptEngineFactory::getEngineName)
					.forEach(System.out::println);
		}
	}

	public static List<ValueMapping> extractMappings(Reader input) throws IOException {
		List<ValueMapping> result = new ArrayList<>();

		List<String> headers = new ArrayList<>();

		CSVUtil.streamCSV(input, h -> headers.addAll(h), (h, l) -> {
			return newMapping(l.get(h.indexOf(LANGUAGE)), l.get(h.indexOf(OLD_FIELD)), l.get(h.indexOf(NEW_FIELD)),
					l.get(h.indexOf(MAPPING)), l.get(h.indexOf(SHOWN)));
		} , l -> result.add(l));

		return result;
	}

	public static List<String> mapLine(List<String> inputHeaders, List<String> line, List<String> previousLine,
			List<String> previousMappedLine, List<ValueMapping> map) throws LineFilteredException {

		Map<String, String> outputValues = new ConcurrentHashMap<>();

		List<String> outputHeaders = map.stream().filter(k -> k.getShown()).map(k -> k.getOutputField())
				.collect(Collectors.toList());
		map.forEach(nextMapping -> {
			String mappedValue = nextMapping.apply(inputHeaders, line, previousLine, previousMappedLine, outputHeaders,
					outputValues);
			outputValues.put(nextMapping.getOutputField(), mappedValue);
		});

		List<String> result = new ArrayList<>(outputHeaders.size());
		outputHeaders.forEach(nextOutput -> result.add(outputValues.getOrDefault(nextOutput, "")));

		return result;
	}

	public static final ValueMapping newMapping(String language, String input, String output, String mapping,
			String shownString) {
		if (output == null || output.isEmpty()) {
			throw new IllegalArgumentException("Output field must not be empty");
		}
		ValueMappingLanguage nextLanguage;
		try {
			nextLanguage = ValueMappingLanguage.valueOf(language.toUpperCase());
		} catch (IllegalArgumentException e) {
			nextLanguage = ValueMappingLanguage.DEFAULT;
		}

		String nextMapping;
		// By default empty mappings do not change the input, and are
		// efficiently dealt with as such
		if (!mapping.isEmpty()) {
			nextMapping = mapping;
		} else {
			nextMapping = nextLanguage.getDefaultMapping();
		}

		boolean shown = !NO.equalsIgnoreCase(shownString);

		ValueMapping result = new ValueMapping(nextLanguage, input, output, nextMapping, shown);

		result.init();

		return result;
	}

	private final ValueMappingLanguage language;

	private final String input;

	private final String output;

	private final String mapping;

	private final boolean shown;

	private transient ScriptEngine scriptEngine;
	private transient CompiledScript compiledScript;

	/**
	 * All creation of ValueMapping objects must be done through the
	 * {@link #newMapping(String, String, String, String)} method.
	 */
	private ValueMapping(ValueMappingLanguage language, String input, String output, String mapping, boolean shown) {
		this.language = language;
		this.input = input;
		this.output = output;
		this.mapping = mapping;
		this.shown = shown;
	}

	private String apply(List<String> inputHeaders, List<String> line, List<String> previousLine,
			List<String> previousMappedLine, List<String> outputHeaders, Map<String, String> mappedLine) {
		int indexOf = inputHeaders.indexOf(getInputField());
		String nextInputValue;
		if (indexOf >= 0) {
			nextInputValue = line.get(indexOf);
		} else {
			// Provide a default input value for these cases. Likely the input
			// field in this case was a set of fields and won't be directly
			// relied upon
			nextInputValue = "";
		}

		// Short circuit if the mapping is a default mapping
		if (this.language == ValueMappingLanguage.DEFAULT || this.language.matchesDefaultMapping(this.mapping)) {
			return nextInputValue;
		}

		if (this.language == ValueMappingLanguage.JAVASCRIPT || this.language == ValueMappingLanguage.GROOVY
				|| this.language == ValueMappingLanguage.LUA) {

			try {
				if (scriptEngine instanceof Invocable) {
					// evaluate script code and access the variable that results
					// from the mapping
					return (String) ((Invocable) scriptEngine).invokeFunction("mapFunction", inputHeaders,
							this.getInputField(), nextInputValue, outputHeaders, this.getOutputField(), line, mappedLine, previousLine,
							previousMappedLine);
				} else if (compiledScript != null) {
					Bindings bindings = scriptEngine.createBindings();
					// inputHeaders, inputField, inputValue, outputField, line
					bindings.put("inputHeaders", inputHeaders);
					bindings.put("inputField", this.getInputField());
					bindings.put("inputValue", nextInputValue);
					bindings.put("outputHeaders", outputHeaders);
					bindings.put("outputField", this.getOutputField());
					bindings.put("line", line);
					bindings.put("mapLine", mappedLine);
					bindings.put("previousLine", previousLine);
					bindings.put("previousMappedLine", previousMappedLine);
					return (String) compiledScript.eval(bindings);
				} else {
					throw new UnsupportedOperationException(
							"Cannot handle results from ScriptEngine.eval that are not Invocable or CompiledScript");
				}
			} catch (ScriptException e) {
				if (e.getCause() != null) {
					if (e.getCause().getMessage().contains(LineFilteredException.class.getCanonicalName())) {
						throw new LineFilteredException(e);
					}
				}
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == ValueMappingLanguage.ACCESS) {
			// Access is currently handled separately, before these mappings are
			// applied, so make this a noop
			return nextInputValue;
		} else if (this.language == ValueMappingLanguage.CSVMERGE) {
			// CSVMerge is currently handled separately, before these mappings
			// are applied, so make this a noop
			return nextInputValue;
		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ValueMapping)) {
			return false;
		}
		ValueMapping other = (ValueMapping) obj;
		if (input == null) {
			if (other.input != null) {
				return false;
			}
		} else if (!input.equals(other.input)) {
			return false;
		}
		if (language != other.language) {
			return false;
		}
		if (mapping == null) {
			if (other.mapping != null) {
				return false;
			}
		} else if (!mapping.equals(other.mapping)) {
			return false;
		}
		if (output == null) {
			if (other.output != null) {
				return false;
			}
		} else if (!output.equals(other.output)) {
			return false;
		}
		if (shown != other.shown) {
			return false;
		}
		return true;
	}

	public String getInputField() {
		return this.input;
	}

	public ValueMappingLanguage getLanguage() {
		return this.language;
	}

	public String getMapping() {
		return this.mapping;
	}

	public String getOutputField() {
		return this.output;
	}

	public boolean getShown() {
		return this.shown;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((input == null) ? 0 : input.hashCode());
		result = prime * result + ((language == null) ? 0 : language.hashCode());
		result = prime * result + ((mapping == null) ? 0 : mapping.hashCode());
		result = prime * result + ((output == null) ? 0 : output.hashCode());
		result = prime * result + (shown ? 1231 : 1237);
		return result;
	}

	private void init() {
		// Short circuit if the mapping is the default mapping and avoid
		// creating an instance of nashorn/groovy/etc. for this mapping
		if (this.language == ValueMappingLanguage.DEFAULT || this.language.matchesDefaultMapping(this.mapping)) {
			return;
		}

		// precompile the function for this mapping for efficiency
		if (this.language == ValueMappingLanguage.JAVASCRIPT) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("javascript");

				StringBuilder javascriptFunction = new StringBuilder();
				javascriptFunction
						.append("var LFE = Java.type(\"com.github.ansell.csv.util.LineFilteredException\"); \n");
				javascriptFunction.append("var Date = Java.type('java.time.LocalDate'); \n");
				javascriptFunction.append("var Format = Java.type('java.time.format.DateTimeFormatter'); \n");
				javascriptFunction.append("var dateMatches = function(format, date) { try {\n format.parse(date); \n return true; \n } catch(e) { } \n return false; }; \n");
				javascriptFunction.append("var dateConvert = function(inputFormat, outputFormat, dateValue) { return Date.parse(dateValue, inputFormat).format(outputFormat); }; \n");
				javascriptFunction.append("var filter = function() { throw new LFE(); }; \n");
				javascriptFunction.append(
						"var columnFunction = function(searchHeader, inputHeaders, line) { return line.get(inputHeaders.indexOf(searchHeader)); };\n");
				javascriptFunction.append(
						"var columnFunctionMap = function(searchHeader, mapLine) { return mapLine.get(searchHeader); };\n");
				javascriptFunction.append(
						"var mapFunction = function(inputHeaders, inputField, inputValue, outputHeaders, outputField, line, mapLine, previousLine, previousMappedLine) { ");
				javascriptFunction.append(
						"    var col = function(searchHeader) { \n return columnFunction(searchHeader, inputHeaders, line); }; \n ");
				javascriptFunction.append(
						"    var outCol = function(searchHeader) { \n return columnFunctionMap(searchHeader, mapLine); }; \n ");
				javascriptFunction.append(this.mapping);
				javascriptFunction.append(" }; \n");

				scriptEngine.eval(javascriptFunction.toString());
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == ValueMappingLanguage.GROOVY) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("groovy");

				scriptEngine
						.eval("def mapFunction(inputHeaders, inputField, inputValue, outputHeaders, outputField, line, mapLine, previousLine, previousMappedLine) {  "
								+ this.mapping + " }");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == ValueMappingLanguage.LUA) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("lua");

				compiledScript = ((Compilable) scriptEngine).compile(this.mapping);
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == ValueMappingLanguage.ACCESS) {

		} else if (this.language == ValueMappingLanguage.CSVMERGE) {

		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
	}

	@Override
	public String toString() {
		return "ValueMapping [language=" + language + ", input=" + input + ", output=" + output + ", mapping=" + mapping
				+ ", shown=" + shown + "]";
	}
}
