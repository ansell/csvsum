/**
 * 
 */
package com.github.ansell.csvsum;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Maps from one CSV file to another based on the supplied mapping definitions.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVMapper {

	/**
	 * Private constructor for static only class
	 */
	private CSVMapper() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be mapped.");
		final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg().ofType(File.class).required()
				.describedAs("The mapping file.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class)
				.describedAs("The mapped CSV file, or the console if not specified.");

		OptionSet options = null;

		try {
			options = parser.parse(args);
		} catch (final OptionException e) {
			System.out.println(e.getMessage());
			parser.printHelpOn(System.out);
			throw e;
		}

		if (options.has(help)) {
			parser.printHelpOn(System.out);
			return;
		}

		final Path inputPath = input.value(options).toPath();
		if (!Files.exists(inputPath)) {
			throw new FileNotFoundException("Could not find input CSV file: " + inputPath.toString());
		}

		final Path mappingPath = mapping.value(options).toPath();
		if (!Files.exists(mappingPath)) {
			throw new FileNotFoundException("Could not find mappng CSV file: " + mappingPath.toString());
		}

		final Writer writer;
		if (options.has(output)) {
			writer = Files.newBufferedWriter(output.value(options).toPath());
		} else {
			writer = new BufferedWriter(new OutputStreamWriter(System.out));
		}

		Map<String, CSVMapping> map = extractMappings(Files.newBufferedReader(mappingPath));
		runMapper(Files.newBufferedReader(inputPath), map, writer);

	}

	private static void runMapper(Reader input, Map<String, CSVMapping> map, Writer output) throws ScriptException {
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine engine = manager.getEngineByName("nashorn");

		List<String> inputList = new ArrayList<>();
		inputList.add("item 1");
		inputList.add("item 2");
		engine.put("input", inputList);
		List<String> outputList = new ArrayList<>();
		engine.put("output", outputList);

		// evaluate JavaScript code and access the variable
		engine.eval("for each (var nextInput in input) { output.add(nextInput); }");

		System.out.println(outputList);

		throw new UnsupportedOperationException("TODO: Implement me!");
	}

	private static Map<String, CSVMapping> extractMappings(Reader input) {
		throw new UnsupportedOperationException("TODO: Implement me!");
	}

}
