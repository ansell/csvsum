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
package com.github.ansell.csv.sum;

import java.io.BufferedReader;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.io.output.NullWriter;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.CSVStreamException;
import com.github.ansell.csv.stream.JSONStream;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Summarises JSON files to easily debug and identify likely parse issues before
 * pushing them through a more heavy tool or process.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JSONSummariser {

	/**
	 * The default number of samples to include for each field in the summarised
	 * CSV.
	 */
	public static final int DEFAULT_SAMPLE_COUNT = 20;

	/**
	 * We are a streaming summariser, and do not store the raw original lines. Only
	 * unique, non-empty, values are stored in the valueCounts map for uniqueness
	 * summaries
	 */
	private static final Consumer<List<String>> NULL_CONSUMER = l -> {
	};

	/**
	 * Header used in the defaults-and-paths file to denote the field name in the
	 * output to use for the values discovered by the matching relativePath.
	 */
	private static final String FIELD = "field";

	/**
	 * Header used in the defaults-and-paths file to denote a default value to use
	 * for a field if there is none found in the discovered objects.
	 */
	private static final String DEFAULT = "default";

	/**
	 * Header used in the defaults-and-paths file to denote the {@link JsonPointer}
	 * to use to find the field inside of each discovered object.
	 */
	private static final String RELATIVE_PATH = "relativePath";

	/**
	 * Private constructor for static only class
	 */
	private JSONSummariser() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input JSON file to be summarised.");
		final OptionSpec<File> fieldsFile = parser.accepts("fields").withRequiredArg().ofType(File.class).required()
				.describedAs(
						"A file which contains a row for each field to be found, including a compulsory path and any optional default value for the field.");
		final OptionSpec<String> basePathOption = parser.accepts("base-path").withRequiredArg().ofType(String.class)
				.describedAs("The base path in the JSON document to locate the array of objects to be summarised")
				.defaultsTo("/");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class)
				.describedAs("The output file, or the console if not specified.");
		final OptionSpec<File> outputMappingTemplate = parser.accepts("output-mapping").withRequiredArg()
				.ofType(File.class).describedAs("The output mapping template file if it needs to be generated.");
		final OptionSpec<Integer> samplesToShow = parser.accepts("samples").withRequiredArg().ofType(Integer.class)
				.defaultsTo(DEFAULT_SAMPLE_COUNT).describedAs(
						"The maximum number of sample values for each field to include in the output, or -1 to dump all sample values for each field.");
		final OptionSpec<Boolean> showSampleCounts = parser.accepts("show-sample-counts").withRequiredArg()
				.ofType(Boolean.class).defaultsTo(Boolean.FALSE)
				.describedAs("Set to true to add counts for each of the samples shown after the sample display value.");
		final OptionSpec<Boolean> debug = parser.accepts("debug").withRequiredArg().ofType(Boolean.class)
				.defaultsTo(Boolean.FALSE).describedAs("Set to true to debug.");

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

		final Path outputMappingPath = options.has(outputMappingTemplate)
				? outputMappingTemplate.value(options).toPath()
				: null;
		if (options.has(outputMappingTemplate) && Files.exists(outputMappingPath)) {
			throw new FileNotFoundException(
					"Output mapping template file already exists: " + outputMappingPath.toString());
		}

		final Writer writer;
		if (options.has(output)) {
			writer = Files.newBufferedWriter(output.value(options).toPath());
		} else {
			writer = new BufferedWriter(new OutputStreamWriter(System.out));
		}

		int samplesToShowInt = samplesToShow.value(options);
		boolean debugBoolean = debug.value(options);

		// Defaults to null, with any strings in the file overriding that
		Map<String, String> defaultsMap = new LinkedHashMap<>();
		Map<String, JsonPointer> pathsMap = new LinkedHashMap<>();
		if (options.has(fieldsFile)) {
			// Files.copy(fieldsFile.value(options).toPath(), System.out);
			try (final BufferedReader newBufferedReader = Files
					.newBufferedReader(fieldsFile.value(options).toPath());) {
				parseFieldsMap(newBufferedReader, defaultsMap, pathsMap);
			}
		}

		if (pathsMap.isEmpty()) {
			throw new RuntimeException("No fields were found");
		}

		JsonPointer basePath = JsonPointer.compile(basePathOption.value(options));

		if (debugBoolean) {
			System.out.println("Running summarise on: " + inputPath + " samples=" + samplesToShowInt);
		}

		ObjectMapper inputMapper = new ObjectMapper();

		try (final BufferedReader newBufferedReader = Files.newBufferedReader(inputPath);
				final Writer mappingWriter = options.has(outputMappingTemplate)
						? Files.newBufferedWriter(outputMappingPath)
						: NullWriter.NULL_WRITER) {
			runSummarise(newBufferedReader, inputMapper, writer, mappingWriter, samplesToShowInt,
					showSampleCounts.value(options), debugBoolean, defaultsMap, basePath, pathsMap);
		}
	}

	/**
	 * Parse field definitions from the input reader into the defaults map and the
	 * paths map.
	 * 
	 * @param inputReader
	 *            The {@link Reader} containing the CSV file with the field
	 *            definitions.
	 * @param defaultsMap
	 *            The {@link Map} to store the defaults into, keyed by field name.
	 * @param pathsMap
	 *            The {@link Map} to store the relative {@link JsonPointer} paths
	 *            into, keyed by the field name.
	 * @throws IOException
	 *             If there is an issue accessing the resource.
	 * @throws CSVStreamException
	 *             If there is an issue with the CSV syntax.
	 */
	public static void parseFieldsMap(final Reader inputReader, final Map<String, String> defaultsMap,
			final Map<String, JsonPointer> pathsMap) throws IOException, CSVStreamException {
		CSVStream.parse(inputReader, h -> {
			// TODO: Validate the headers as expected
		}, (h, l) -> {
			defaultsMap.put(l.get(h.indexOf(FIELD)), l.get(h.indexOf(DEFAULT)));
			pathsMap.put(l.get(h.indexOf(FIELD)), JsonPointer.compile(l.get(h.indexOf(RELATIVE_PATH))));
			return l;
		}, l -> {
		}, null, CSVStream.DEFAULT_HEADER_COUNT);
	}

	/**
	 * Summarise the JSON file from the input {@link Reader} and emit the summary
	 * CSV file to the output {@link Writer}, including the given maximum number of
	 * sample values in the summary for each field.
	 * 
	 * @param input
	 *            The input JSON file, as a {@link Reader}.
	 * @param inputMapper
	 *            The ObjectMapper to use to parse the file into memory
	 * @param output
	 *            The output CSV file as a {@link Writer}.
	 * @param mappingOutput
	 *            The output mapping template file as a {@link Writer}.
	 * @param maxSampleCount
	 *            The maximum number of sample values in the summary for each field.
	 *            Set to -1 to include all unique values for each field.
	 * @param showSampleCounts
	 *            Show counts next to sample values
	 * @param debug
	 *            Set to true to add debug statements.
	 * @param defaultValues
	 *            A Map of default values to substitute during the summarise process
	 *            if there is no value given for the matching field in the CSV file.
	 *            The length of this list must either be 0 or the same as the number
	 *            of fields.
	 * @param basePath
	 *            The path to go to before checking the field paths (only supports a
	 *            single point of entry at this point in time). Set to "/" to start
	 *            at the top of the document. If the basePath points to an array,
	 *            each of the array elements are matched separately with the
	 *            fieldRelativePaths. If it points to an object, the object is
	 *            directly matched to obtain a single result row. Otherwise an
	 *            exception is thrown.
	 * @param fieldRelativePaths
	 *            The relative paths underneath the basePath to select field values
	 *            from.
	 * @throws IOException
	 *             If there is an error reading or writing.
	 */
	public static void runSummarise(final Reader input, final ObjectMapper inputMapper, final Writer output,
			final Writer mappingOutput, final int maxSampleCount, final boolean showSampleCounts, final boolean debug,
			final Map<String, String> defaultValues, JsonPointer basePath, Map<String, JsonPointer> fieldRelativePaths)
			throws IOException {
		final JDefaultDict<String, AtomicInteger> emptyCounts = new JDefaultDict<>(k -> new AtomicInteger());
		final JDefaultDict<String, AtomicInteger> nonEmptyCounts = new JDefaultDict<>(k -> new AtomicInteger());
		// Default to true, and set to false if a non-integer is detected. The
		// special case of no values being found is handled in the write method
		// and false is used
		final JDefaultDict<String, AtomicBoolean> possibleIntegerFields = new JDefaultDict<>(
				k -> new AtomicBoolean(true));
		// Default to true, and set to false if a non-double is detected. The
		// special case of no values being found is handled in the write method
		// and false is used
		final JDefaultDict<String, AtomicBoolean> possibleDoubleFields = new JDefaultDict<>(
				k -> new AtomicBoolean(true));
		final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts = new JDefaultDict<String, JDefaultDict<String, AtomicInteger>>(
				k -> new JDefaultDict<>(l -> new AtomicInteger()));
		final AtomicInteger rowCount = new AtomicInteger();

		final List<String> headers = parseForSummarise(input, inputMapper, emptyCounts, nonEmptyCounts,
				possibleIntegerFields, possibleDoubleFields, valueCounts, rowCount, defaultValues, basePath,
				fieldRelativePaths);

		CSVSummariser.writeForSummarise(maxSampleCount, emptyCounts, nonEmptyCounts, possibleIntegerFields,
				possibleDoubleFields, valueCounts, headers, rowCount, showSampleCounts, output, mappingOutput);
	}

	/**
	 * Parse the given inputs to in-memory maps to allow for summarisation.
	 * 
	 * @param input
	 *            The {@link Reader} containing the inputs to be summarised.
	 * @param inputMapper
	 *            The {@link ObjectMapper} to use to parse the file into memory
	 * @param emptyCounts
	 *            A {@link JDefaultDict} to be populated with empty counts for each
	 *            field
	 * @param nonEmptyCounts
	 *            A {@link JDefaultDict} to be populated with non-empty counts for
	 *            each field
	 * @param possibleIntegerFields
	 *            A {@link JDefaultDict} to be populated with false if a non-integer
	 *            value is detected in a field
	 * @param possibleDoubleFields
	 *            A {@link JDefaultDict} to be populated with false if a non-double
	 *            value is detected in a field
	 * @param valueCounts
	 *            A {@link JDefaultDict} to be populated with false if a non-integer
	 *            value is detected in a field
	 * @param rowCount
	 *            An {@link AtomicInteger} used to track the total number of rows
	 *            processed.
	 * @param defaultValues
	 *            A Map that is either empty, signifying there are no default values
	 *            known, or exactly the same length as each row in the CSV file
	 *            being parsed. If the values for a field are empty/missing, and a
	 *            non-null, non-empty value appears in this list, it will be
	 *            substituted in when calculating the statistics.
	 * @param basePath
	 *            The path to go to before checking the field paths (only supports a
	 *            single point of entry at this point in time). Set to "/" to start
	 *            at the top of the document. If the basePath points to an array,
	 *            each of the array elements are matched separately with the
	 *            fieldRelativePaths. If it points to an object, the object is
	 *            directly matched to obtain a single result row. Otherwise an
	 *            exception is thrown.
	 * @param fieldRelativePaths
	 *            The relative paths underneath the basePath to select field values
	 *            from.
	 * @return The list of headers that were either overridden or found in the file
	 * @throws IOException
	 *             If there is an error reading from the file
	 * @throws CSVStreamException
	 *             If there is a problem processing the JSON content
	 */
	private static List<String> parseForSummarise(final Reader input, final ObjectMapper inputMapper,
			final JDefaultDict<String, AtomicInteger> emptyCounts,
			final JDefaultDict<String, AtomicInteger> nonEmptyCounts,
			final JDefaultDict<String, AtomicBoolean> possibleIntegerFields,
			final JDefaultDict<String, AtomicBoolean> possibleDoubleFields,
			final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts, final AtomicInteger rowCount,
			Map<String, String> defaultValues, JsonPointer basePath, Map<String, JsonPointer> fieldRelativePaths)
			throws IOException, CSVStreamException {
		final long startTime = System.currentTimeMillis();
		final BiFunction<List<String>, List<String>, List<String>> summariseFunction = CSVSummariser
				.getSummaryFunctionWithStartTime(emptyCounts, nonEmptyCounts, possibleIntegerFields,
						possibleDoubleFields, valueCounts, rowCount, startTime);
		return parseForSummarise(input, inputMapper, defaultValues, summariseFunction, basePath, fieldRelativePaths);
	}

	/**
	 * Parse the given inputs to in-memory maps to allow for summarisation.
	 * 
	 * @param input
	 *            The {@link Reader} containing the inputs to be summarised.
	 * @param inputMapper
	 *            The CsvMapper to use to parse the file into memory
	 * @param defaultValues
	 *            A list that is either empty, signifying there are no default
	 *            values known, or exactly the same length as each row in the CSV
	 *            file being parsed. If the values for a field are empty/missing,
	 *            and a non-null, non-empty value appears in this list, it will be
	 *            substituted in when calculating the statistics.
	 * @param summariseFunction
	 *            The function which will perform the summarisation
	 * @param basePath
	 *            The path to go to before checking the field paths (only supports a
	 *            single point of entry at this point in time). Set to "/" to start
	 *            at the top of the document. If the basePath points to an array,
	 *            each of the array elements are matched separately with the
	 *            fieldRelativePaths. If it points to an object, the object is
	 *            directly matched to obtain a single result row. Otherwise an
	 *            exception is thrown.
	 * @param fieldRelativePaths
	 *            The relative paths underneath the basePath to select field values
	 *            from.
	 * @return The list of headers that were either overridden or found in the file
	 * @throws IOException
	 *             If there is an error reading from the file
	 * @throws CSVStreamException
	 *             If there is a problem processing the CSV content
	 */
	private static List<String> parseForSummarise(final Reader input, final ObjectMapper inputMapper,
			Map<String, String> defaultValues, BiFunction<List<String>, List<String>, List<String>> summariseFunction,
			JsonPointer basePath, Map<String, JsonPointer> fieldRelativePaths) throws IOException, CSVStreamException {
		// This will be populated with whatever is recognised as the headers when the
		// input is parsed, so we can return it from this function
		final List<String> headers = new ArrayList<>();
		JSONStream.parse(input, h -> headers.addAll(h), summariseFunction, NULL_CONSUMER, basePath, fieldRelativePaths,
				defaultValues, inputMapper);
		return headers;
	}

}
