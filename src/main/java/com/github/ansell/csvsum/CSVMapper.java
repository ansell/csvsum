/**
 * 
 */
package com.github.ansell.csvsum;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.jdefaultdict.JDefaultDict;

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

		JDefaultDict<String, List<CSVMapping>> map = extractMappings(Files.newBufferedReader(mappingPath));

		runMapper(Files.newBufferedReader(inputPath), map, writer);

	}

	private static void runMapper(Reader input, Map<String, List<CSVMapping>> map, Writer output)
			throws ScriptException, IOException {

		Function<List<CSVMapping>, List<String>> outputFields = k -> k.stream().map(e -> e.getOutputField())
				.collect(Collectors.toList());

		List<String> outputHeaders = map.values().stream().map(outputFields).reduce(new ArrayList<String>(), (k, l) -> {
			k.addAll(l);
			return k;
		} , (a, b) -> {
			a.addAll(b);
			return a;
		});

		final CsvSchema schema = CSVUtil.buildSchema(outputHeaders);

		try (final SequenceWriter csvWriter = CSVUtil.newCSVWriter(output, schema);) {
			List<String> inputHeaders = new ArrayList<>();
			CSVUtil.streamCSV(input, h -> inputHeaders.addAll(h), (h, l) -> {
				return CSVMapping.mapLine(h, outputHeaders, l, map);
			} , l -> {
				// Write out all of the mapped lines for this original line in
				// the original CSV file
				try {
					csvWriter.write(l);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

		}
	}

	private static JDefaultDict<String, List<CSVMapping>> extractMappings(Reader input) throws IOException {
		JDefaultDict<String, List<CSVMapping>> result = new JDefaultDict<>(k -> new ArrayList<>());

		List<String> headers = new ArrayList<>();

		CSVUtil.streamCSV(input, h -> headers.addAll(h), (h, l) -> {
			return CSVMapping.getMapping(l.get(h.indexOf(CSVMapping.LANGUAGE)), l.get(h.indexOf(CSVMapping.OLD_FIELD)),
					l.get(h.indexOf(CSVMapping.NEW_FIELD)), l.get(h.indexOf(CSVMapping.MAPPING)));
		} , l -> {
			result.get(l.getInputField()).add(l);
		});

		return result;
	}

}
