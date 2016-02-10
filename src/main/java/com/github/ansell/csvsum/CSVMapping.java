/**
 * 
 */
package com.github.ansell.csvsum;

/**
 * A mapping definition from an original CSV field to an output CSV field.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
class CSVMapping {

	enum CSVMappingLanguage {
		JAVASCRIPT
	}

	private CSVMappingLanguage language;
	private String input;
	private String output;
	private String mapping;
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
		result.mapping = mapping;

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
}
