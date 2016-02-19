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

	public enum ValueMappingLanguage {
		DEFAULT, JAVASCRIPT, GROOVY, LUA, ACCESS
	}

	/**
	 * The default mapping if none is specified in the mapping file.
	 */
	protected static final String DEFAULT_MAPPING = "inputValue";

	public static final String LANGUAGE = "Language";

	public static final String OLD_FIELD = "OldField";

	public static final String NEW_FIELD = "NewField";
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
					l.get(h.indexOf(MAPPING)));
		} , l -> result.add(l));

		return result;
	}

	public static List<String> mapLine(List<String> inputHeaders, List<String> outputHeaders, List<String> line,
			List<ValueMapping> map) {

		if (outputHeaders.size() != map.size()) {
			throw new IllegalArgumentException("The number of mappings must match the number of output headers");
		}

		Map<String, String> outputValues = new ConcurrentHashMap<>();
		List<String> result = new ArrayList<>();

		// Note, empirically, it seems about 50% faster with a limited number of
		// cores to do a serial mapping, not a parallel mapping
		// map.parallelStream().forEach(nextMapping -> {
		// for (ValueMapping nextMapping : map) {
		map.forEach(nextMapping -> {
			String mappedValue = nextMapping.apply(inputHeaders, line);
			outputValues.put(nextMapping.getOutputField(), mappedValue);
		});

		outputHeaders.forEach(nextOutput -> result.add(outputValues.get(nextOutput)));

		return result;
	}

	public static final ValueMapping newMapping(String language, String input, String output, String mapping) {
		ValueMappingLanguage nextLanguage;
		try {
			nextLanguage = ValueMappingLanguage.valueOf(language.toUpperCase());
		} catch (IllegalArgumentException e) {
			nextLanguage = ValueMappingLanguage.DEFAULT;
		}

		if (nextLanguage == null) {
			throw new IllegalArgumentException("No mapping language found for: " + language);
		}

		String nextMapping;
		// By default empty mappings do not change the input, and are
		// efficiently dealt with as such
		if (!mapping.isEmpty()) {
			nextMapping = mapping;
		} else {
			nextMapping = DEFAULT_MAPPING;
		}

		ValueMapping result = new ValueMapping(nextLanguage, input, output, nextMapping);

		result.init();

		return result;
	}

	private final ValueMappingLanguage language;

	private final String input;

	private final String output;

	private final String mapping;

	private transient ScriptEngine scriptEngine;
	private transient CompiledScript compiledScript;

	/**
	 * All creation of ValueMapping objects must be done through the
	 * {@link #newMapping(String, String, String, String)} method.
	 */
	private ValueMapping(ValueMappingLanguage language, String input, String output, String mapping) {
		this.language = language;
		this.input = input;
		this.output = output;
		this.mapping = mapping;
	}

	private String apply(List<String> inputHeaders, List<String> line) {
		String nextInputValue = line.get(inputHeaders.indexOf(getInputField()));

		// Short circuit if the mapping is the default mapping
		if (this.mapping.equalsIgnoreCase(DEFAULT_MAPPING) || this.language == ValueMappingLanguage.DEFAULT) {
			return nextInputValue;
		}

		if (this.language == ValueMappingLanguage.JAVASCRIPT || this.language == ValueMappingLanguage.GROOVY
				|| this.language == ValueMappingLanguage.LUA) {

			try {
				if (scriptEngine instanceof Invocable) {
					// evaluate script code and access the variable that results
					// from the mapping
					return (String) ((Invocable) scriptEngine).invokeFunction("mapFunction", inputHeaders,
							this.getInputField(), nextInputValue, this.getOutputField(), line);
				} else if (compiledScript != null) {
					Bindings bindings = scriptEngine.createBindings();
					// inputHeaders, inputField, inputValue, outputField, line
					bindings.put("inputHeaders", inputHeaders);
					bindings.put("inputField", this.getInputField());
					bindings.put("inputValue", nextInputValue);
					bindings.put("outputField", this.getOutputField());
					bindings.put("line", line);
					return (String) compiledScript.eval(bindings);
				} else {
					throw new UnsupportedOperationException(
							"Cannot handle results from ScriptEngine.eval that are not Invocable or CompiledScript");
				}
			} catch (ScriptException | NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((input == null) ? 0 : input.hashCode());
		result = prime * result + ((language == null) ? 0 : language.hashCode());
		result = prime * result + ((mapping == null) ? 0 : mapping.hashCode());
		result = prime * result + ((output == null) ? 0 : output.hashCode());
		return result;
	}

	private void init() {
		// Short circuit if the mapping is the default mapping and avoid
		// creating an instance of nashorn/groovy/etc. for this mapping
		if (this.mapping.equalsIgnoreCase(DEFAULT_MAPPING) || this.language == ValueMappingLanguage.DEFAULT) {
			return;
		}

		// precompile the function for this mapping for efficiency
		if (this.language == ValueMappingLanguage.JAVASCRIPT) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("javascript");

				scriptEngine
						.eval("var mapFunction = function(inputHeaders, inputField, inputValue, outputField, line) { "
								+ this.mapping + " };");
			} catch (ScriptException e) {
				throw new RuntimeException(e);
			}
		} else if (this.language == ValueMappingLanguage.GROOVY) {
			try {
				scriptEngine = SCRIPT_MANAGER.getEngineByName("groovy");

				scriptEngine.eval("def mapFunction(inputHeaders, inputField, inputValue, outputField, line) {  "
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
		} else {
			throw new UnsupportedOperationException("Mapping language not supported: " + this.language);
		}
	}
}