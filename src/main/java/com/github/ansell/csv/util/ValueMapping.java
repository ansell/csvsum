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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.jdefaultdict.JDefaultDict;

/**
 * A mapping definition from an original CSV field to an output CSV field.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ValueMapping {

	private static final String NO = "no";

	/**
	 * The default mapping if none is specified in the mapping file.
	 */
	protected static final String DEFAULT_MAPPING = "inputValue";

	public enum ValueMappingLanguage {
		DEFAULT(ValueMapping.DEFAULT_MAPPING),

		JAVASCRIPT("return inputValue;"),

		GROOVY("inputValue"),

		LUA("return inputValue"),

		ACCESS(""),

		CSVJOIN(""),

		DBSCHEMA(""),

		;

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

	public static final String OLD_FIELD = "OldField";
	public static final String NEW_FIELD = "NewField";
	public static final String SHOWN = "Shown";
	public static final String DEFAULT = "Default";
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

		CSVStream.parse(input, h -> {
		}, (h, l) -> {
			// The default field is optional to allow for backwards compatibility
			String nextDefault = "";
			if(h.indexOf(DEFAULT) >= 0) {
				nextDefault = l.get(h.indexOf(DEFAULT));
			}
			return newMapping(l.get(h.indexOf(LANGUAGE)), l.get(h.indexOf(OLD_FIELD)), l.get(h.indexOf(NEW_FIELD)),
					l.get(h.indexOf(MAPPING)), l.get(h.indexOf(SHOWN)), nextDefault);
		}, l -> result.add(l));

		return Collections.unmodifiableList(result);
	}

	public static List<String> mapLine(List<String> inputHeaders, List<String> line, List<String> previousLine,
			List<String> previousMappedLine, List<ValueMapping> map, JDefaultDict<String, Set<String>> primaryKeys,
			int lineNumber, int filteredLineNumber, BiConsumer<List<String>, List<String>> mapLineConsumer)
			throws LineFilteredException {

		List<String> outputHeaders = map.stream().filter(k -> k.getShown()).map(k -> k.getOutputField())
				.collect(Collectors.toList());
		Map<String, String> defaultValues = map.stream().filter(k -> k.getShown())
				.collect(Collectors.toMap(ValueMapping::getOutputField, ValueMapping::getDefaultValue));
		
		return mapLine(inputHeaders, line, previousLine, previousMappedLine, map, primaryKeys, lineNumber, filteredLineNumber, mapLineConsumer, outputHeaders, defaultValues);
	}
	
	public static List<String> mapLine(List<String> inputHeaders, List<String> line, List<String> previousLine,
			List<String> previousMappedLine, List<ValueMapping> map, JDefaultDict<String, Set<String>> primaryKeys,
			int lineNumber, int filteredLineNumber, BiConsumer<List<String>, List<String>> mapLineConsumer, List<String> outputHeaders, Map<String, String> defaultValues)
			throws LineFilteredException {

		HashMap<String, String> outputValues = new HashMap<>(map.size(), 0.75f);

		map.forEach(nextMapping -> {
			String mappedValue = nextMapping.apply(inputHeaders, line, previousLine, previousMappedLine, outputHeaders,
					outputValues, primaryKeys, lineNumber, filteredLineNumber, mapLineConsumer);
			outputValues.put(nextMapping.getOutputField(), mappedValue);
		});

		List<String> result = new ArrayList<>(outputHeaders.size());
		outputHeaders.forEach(nextOutput -> result.add(outputValues.getOrDefault(nextOutput, defaultValues.getOrDefault(nextOutput, ""))));

		outputValues.clear();

		return result;
	}

	public static final ValueMapping newMapping(String language, String input, String output, String mapping,
			String shownString, String nextDefault) {
		if (output == null || output.isEmpty()) {
			throw new IllegalArgumentException("Output field must not be empty");
		}
		
		// Ignore null values for default and replace with empty string
		if (nextDefault == null) {
			nextDefault = "";
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

		ValueMapping result = new ValueMapping(nextLanguage, input, output, nextMapping, shown, nextDefault);

		result.init();

		return result;
	}

	private final ValueMappingLanguage language;

	private final String input;

	private final String output;

	private final String mapping;

	private final boolean shown;

	private final String theDefault;

	private final String[] destFields;
	private final String[] sourceFields;

	private transient ScriptEngine scriptEngine;
	private transient CompiledScript compiledScript;

	/**
	 * All creation of ValueMapping objects must be done through the
	 * {@link #newMapping(String, String, String, String)} method.
	 */
	private ValueMapping(ValueMappingLanguage language, String input, String output, String mapping, boolean shown, String nextDefault) {
		this.language = language;
		this.input = input.intern();
		this.output = output.intern();
		this.mapping = mapping.intern();
		this.shown = shown;
		this.theDefault = nextDefault;
		this.destFields = CSVUtil.COMMA_PATTERN.split(this.mapping);
		this.sourceFields = CSVUtil.COMMA_PATTERN.split(this.input);
	}

	private String apply(List<String> inputHeaders, List<String> line, List<String> previousLine,
			List<String> previousMappedLine, List<String> outputHeaders, Map<String, String> mappedLine,
			JDefaultDict<String, Set<String>> primaryKeys, int lineNumber, int filteredLineNumber,
			BiConsumer<List<String>, List<String>> mapLineConsumer) {
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
							this.getInputField(), nextInputValue, outputHeaders, this.getOutputField(), line,
							mappedLine, previousLine, previousMappedLine, primaryKeys, lineNumber, filteredLineNumber,
							mapLineConsumer, theDefault);
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
					bindings.put("primaryKeys", primaryKeys);
					bindings.put("lineNumber", lineNumber);
					bindings.put("filteredLineNumber", filteredLineNumber);
					bindings.put("mapLineConsumer", mapLineConsumer);
					bindings.put("defaultValue", theDefault);
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
		} else if (this.language == ValueMappingLanguage.CSVJOIN) {
			// CSVJoin is currently handled separately, before these mappings
			// are applied, so make this a noop
			return nextInputValue;
		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
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

	public String getDefaultValue() {
		return this.theDefault;
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
				javascriptFunction.append("var Integer = Java.type('java.lang.Integer'); \n");
				javascriptFunction.append("var Double = Java.type('java.lang.Double'); \n");
				javascriptFunction.append("var Long = Java.type('java.lang.Long'); \n");
				javascriptFunction.append("var LocalDate = Java.type('java.time.LocalDate'); \n");
				javascriptFunction.append("var LocalDateTime = Java.type('java.time.LocalDateTime'); \n");
				javascriptFunction.append("var LocalTime = Java.type('java.time.LocalTime'); \n");
				javascriptFunction.append("var TimeUnit = Java.type('java.util.concurrent.TimeUnit'); \n");
				javascriptFunction.append("var Locale = Java.type('java.util.Locale'); \n");
				javascriptFunction.append("var Format = Java.type('java.time.format.DateTimeFormatter'); \n");
				javascriptFunction.append(
						"var DateTimeFormatterBuilder = Java.type('java.time.format.DateTimeFormatterBuilder'); \n");
				javascriptFunction.append("var ChronoUnit = Java.type('java.time.temporal.ChronoUnit'); \n");
				javascriptFunction.append("var Math = Java.type('java.lang.Math'); \n");
				javascriptFunction.append("var String = Java.type('java.lang.String'); \n");
				javascriptFunction.append("var MessageDigest = Java.type('java.security.MessageDigest'); \n");
				javascriptFunction.append("var BigInteger = Java.type('java.math.BigInteger'); \n");
				javascriptFunction.append("var Arrays = Java.type('java.util.Arrays'); \n");
				javascriptFunction.append("var UTM = Java.type('com.github.ansell.shp.UTM'); \n");
				javascriptFunction.append("var WGS84 = Java.type('com.github.ansell.shp.WGS84'); \n");
				javascriptFunction.append("var JSONUtil = Java.type('com.github.ansell.csv.util.JSONUtil'); \n");
				javascriptFunction.append("var Paths = Java.type('java.nio.file.Paths'); \n");
				javascriptFunction.append("var JsonPointer = Java.type('com.fasterxml.jackson.core.JsonPointer'); \n");
				javascriptFunction.append("var Thread = Java.type('java.lang.Thread'); \n");
				javascriptFunction.append("var System = Java.type('java.lang.System'); \n");
				javascriptFunction.append("var sleep = function(sleepTime) { Thread.sleep(sleepTime); }; \n");
				javascriptFunction.append(
						"var digest = function(value, algorithm, formatPattern) { if(!algorithm) { algorithm = \"SHA-256\"; } if(!formatPattern) { formatPattern = \"%064x\";} var md = MessageDigest.getInstance(algorithm); md.update(value.getBytes(\"UTF-8\")); var digestValue = md.digest(); return String.format(formatPattern, new BigInteger(1, digestValue));}; \n");
				javascriptFunction.append(
						"var replaceLineEndingsWith = function(value, replacement) { if(!replacement) { replacement = \"\"; } return value.replaceAll(\"\\r\\n|\\r|\\n\", replacement); }; \n");
				javascriptFunction.append(
						"var printErr = function(message) { System.err.println(message); }; \n");
				javascriptFunction.append(
						"var printTimings = function(startTime, rowCount) { var secondsSinceStart = (System.currentTimeMillis() - startTime) / 1000.0; System.out.printf(\"%d\\tSeconds since start: %f\\tRecords per second: %f%n\", nextLineNumber, secondsSinceStart, (nextLineNumber / secondsSinceStart)); }; \n");
				javascriptFunction.append(
						"var newDateFormat = function(formatPattern) { return new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(formatPattern).toFormatter(Locale.US); }; \n");
				javascriptFunction.append(
						"var dateMatches = function(dateValue, format) { try {\n format.parse(dateValue); \n return true; \n } catch(e) { } \n return false; }; \n");
				javascriptFunction.append(
						"var dateConvert = function(dateValue, inputFormat, outputFormat, parseClass) { if(!parseClass) { parseClass = LocalDate; } return parseClass.parse(dateValue, inputFormat).format(outputFormat); }; \n");
				javascriptFunction.append("var filter = function() { throw new LFE(); }; \n");
				javascriptFunction.append(
						"var columnFunction = function(searchHeader, inputHeaders, line) { return inputHeaders.indexOf(searchHeader) >= 0 ? line.get(inputHeaders.indexOf(searchHeader)) : \"Could not find: \" + searchHeader; };\n");
				javascriptFunction.append(
						"var columnFunctionMap = function(searchHeader, mapLine) { return mapLine.get(searchHeader); };\n");
				javascriptFunction.append(
						"var mapFunction = function(inputHeaders, inputField, inputValue, outputHeaders, outputField, line, mapLine, previousLine, previousMappedLine, primaryKeys, lineNumber, filteredLineNumber, mapLineConsumer, defaultValue) { ");
				javascriptFunction.append(
						"    var primaryKeyBoolean = function(nextPrimaryKey, primaryKeyField) { \n if(!primaryKeyField) { primaryKeyField = \"Primary\"; } \n return primaryKeys.get(primaryKeyField).add(nextPrimaryKey); }; \n ");
				javascriptFunction.append(
						"    var primaryKeyFilter = function(nextPrimaryKey, primaryKeyField) { \n if(!primaryKeyField) { primaryKeyField = \"Primary\"; } \n return primaryKeys.get(primaryKeyField).add(nextPrimaryKey) ? nextPrimaryKey : filter(); }; \n ");
				javascriptFunction.append(
						"    var col = function(searchHeader) { \n return columnFunction(searchHeader, inputHeaders, line); }; \n ");
				javascriptFunction.append(
						"    var outCol = function(searchHeader) { \n return columnFunctionMap(searchHeader, mapLine); }; \n ");
				javascriptFunction.append(this.mapping);
				javascriptFunction.append(" \n }; \n");

				scriptEngine.eval(javascriptFunction.toString());
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == ValueMappingLanguage.GROOVY) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("groovy");

				scriptEngine
						.eval("def mapFunction(inputHeaders, inputField, inputValue, outputHeaders, outputField, line, mapLine, previousLine, previousMappedLine, primaryKeys, lineNumber, filteredLineNumber, mapLineConsumer, defaultValue) {  "
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

		} else if (this.language == ValueMappingLanguage.CSVJOIN) {

		} else if (this.language == ValueMappingLanguage.DBSCHEMA) {

		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
	}

	@Override
	public String toString() {
		return "ValueMapping [language=" + language + ", input=" + input + ", output=" + output + ", mapping=" + mapping
				+ ", shown=" + shown + "]";
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(destFields);
		result = prime * result + ((input == null) ? 0 : input.hashCode());
		result = prime * result + ((language == null) ? 0 : language.hashCode());
		result = prime * result + ((mapping == null) ? 0 : mapping.hashCode());
		result = prime * result + ((output == null) ? 0 : output.hashCode());
		result = prime * result + (shown ? 1231 : 1237);
		result = prime * result + Arrays.hashCode(sourceFields);
		result = prime * result + ((theDefault == null) ? 0 : theDefault.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
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
		if (!Arrays.equals(destFields, other.destFields)) {
			return false;
		}
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
		if (!Arrays.equals(sourceFields, other.sourceFields)) {
			return false;
		}
		if (theDefault == null) {
			if (other.theDefault != null) {
				return false;
			}
		} else if (!theDefault.equals(other.theDefault)) {
			return false;
		}
		return true;
	}

	public String[] getDestFields() {
		return this.destFields;
	}

	public String[] getSourceFields() {
		return this.sourceFields;
	}
}
