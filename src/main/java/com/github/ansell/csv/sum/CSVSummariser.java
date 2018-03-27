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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.io.output.NullWriter;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.CSVStreamException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Summarises CSV files to easily debug and identify likely parse issues before
 * pushing them through a more heavy tool or process.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVSummariser {

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
	 * Private constructor for static only class
	 */
	private CSVSummariser() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be summarised.");
		final OptionSpec<File> overrideHeadersFile = parser.accepts("override-headers-file").withRequiredArg()
				.ofType(File.class).describedAs(
						"A file whose first line contains the headers to use, to override those found in the file.");
		final OptionSpec<Integer> headerLineCount = parser.accepts("header-line-count").withRequiredArg()
				.ofType(Integer.class)
				.describedAs(
						"The number of header lines present in the file. Can be used in conjunction with override-headers-file to substitute a different set of headers")
				.defaultsTo(1);
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
		final OptionSpec<String> separatorCharacterOption = parser.accepts("separator-char").withRequiredArg()
				.ofType(String.class).defaultsTo(",")
				.describedAs("Overrides the default RFC4180 Section 2 column separator character");
		final OptionSpec<String> quoteCharacterOption = parser.accepts("quote-char").withRequiredArg()
				.ofType(String.class).defaultsTo("\"")
				.describedAs("Overrides the default RFC4180 Section 2 quote character");
		final OptionSpec<String> escapeCharacterOption = parser.accepts("escape-char").withRequiredArg()
				.ofType(String.class).defaultsTo("").describedAs(
						"RFC4180 Section 2 does not define escape characters, but some implementations use a different character to the quote character, so support for those can be enabled using this option");

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

		final boolean showSampleCountsBoolean = showSampleCounts.value(options);
		final int samplesToShowInt = samplesToShow.value(options);
		final int headerLineCountInt = headerLineCount.value(options);
		final boolean debugBoolean = debug.value(options);

		// Defaults to null, with any strings in the file overriding that
		AtomicReference<List<String>> overrideHeadersList = new AtomicReference<>();
		if (options.has(overrideHeadersFile)) {
			parseOverrideHeaders(overrideHeadersFile, options, overrideHeadersList);
		}

		if (debugBoolean) {
			System.out.println("Running summarise on: " + inputPath + " samples=" + samplesToShowInt);
		}

		CsvMapper inputMapper = CSVStream.defaultMapper();
		final CsvSchema inputSchema;
		if (!options.has(separatorCharacterOption) && !options.has(quoteCharacterOption)
				&& !options.has(escapeCharacterOption)) {
			inputSchema = CSVStream.defaultSchema();
		} else {
			CsvSchema customSchema = CSVStream.defaultSchema()
					.withColumnSeparator(separatorCharacterOption.value(options).charAt(0));
			if (!quoteCharacterOption.value(options).isEmpty()) {
				char quoteCharChosen = quoteCharacterOption.value(options).charAt(0);
				if (debugBoolean) {
					System.out.println("Setting quote char to: " + quoteCharChosen);
				}
				customSchema = customSchema.withQuoteChar(quoteCharChosen);
			} else {
				customSchema = customSchema.withoutQuoteChar();
			}
			if (!escapeCharacterOption.value(options).isEmpty()) {
				char escapeCharChosen = escapeCharacterOption.value(options).charAt(0);
				if (debugBoolean) {
					System.out.println("Setting escape char to: " + escapeCharChosen);
				}
				customSchema = customSchema.withEscapeChar(escapeCharChosen);
			} else {
				customSchema = customSchema.withoutEscapeChar();
			}
			inputSchema = customSchema;
		}

		try (final BufferedReader newBufferedReader = Files.newBufferedReader(inputPath);
				final Writer mappingWriter = options.has(outputMappingTemplate)
						? Files.newBufferedWriter(outputMappingPath)
						: NullWriter.NULL_WRITER) {
			runSummarise(newBufferedReader, inputMapper, inputSchema, writer, mappingWriter, samplesToShowInt,
					showSampleCountsBoolean, debugBoolean, overrideHeadersList.get(), Collections.emptyList(),
					headerLineCountInt);
		}
	}

	public static void parseOverrideHeaders(final OptionSpec<File> overrideHeadersFile, OptionSet options,
			AtomicReference<List<String>> overrideHeadersList) throws IOException {
		try (final BufferedReader newBufferedReader = Files
				.newBufferedReader(overrideHeadersFile.value(options).toPath());) {
			CSVStream.parse(newBufferedReader, h -> {
				overrideHeadersList.set(h);
			}, (h, l) -> {
				return l;
			}, l -> {
			}, null, CSVStream.DEFAULT_HEADER_COUNT);
		}
	}

	/**
	 * Summarise the CSV file from the input {@link Reader} and emit the summary CSV
	 * file to the output {@link Writer}, including the given maximum number of
	 * sample values in the summary for each field.
	 * 
	 * @param input
	 *            The input CSV file, as a {@link Reader}.
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
	 * @throws IOException
	 *             If there is an error reading or writing.
	 */
	public static void runSummarise(Reader input, Writer output, Writer mappingOutput, int maxSampleCount,
			boolean showSampleCounts, boolean debug) throws IOException {
		runSummarise(input, output, mappingOutput, maxSampleCount, showSampleCounts, debug, null,
				CSVStream.DEFAULT_HEADER_COUNT);
	}

	/**
	 * Summarise the CSV file from the input {@link Reader} and emit the summary CSV
	 * file to the output {@link Writer}, including the given maximum number of
	 * sample values in the summary for each field.
	 * 
	 * @param input
	 *            The input CSV file, as a {@link Reader}.
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
	 * @param overrideHeaders
	 *            A set of headers to override those in the file or null to use the
	 *            headers from the file. If this is null and headerLineCount is set
	 *            to 0, an IllegalArgumentException ill be thrown.
	 * @param headerLineCount
	 *            The number of header lines to expect
	 * @throws IOException
	 *             If there is an error reading or writing.
	 */
	public static void runSummarise(final Reader input, final Writer output, final Writer mappingOutput,
			final int maxSampleCount, final boolean showSampleCounts, final boolean debug,
			final List<String> overrideHeaders, final int headerLineCount) throws IOException {
		final CsvMapper inputMapper = CSVStream.defaultMapper();
		final CsvSchema inputSchema = CSVStream.defaultSchema();
		runSummarise(input, inputMapper, inputSchema, output, mappingOutput, maxSampleCount, showSampleCounts, debug,
				overrideHeaders, Collections.emptyList(), headerLineCount);
	}

	/**
	 * Summarise the CSV file from the input {@link Reader} and emit the summary CSV
	 * file to the output {@link Writer}, including the given maximum number of
	 * sample values in the summary for each field.
	 * 
	 * @param input
	 *            The input CSV file, as a {@link Reader}.
	 * @param inputMapper
	 *            The CsvMapper to use to parse the file into memory
	 * @param inputSchema
	 *            The CsvSchema to use to help the mapper parse the file into memory
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
	 * @param overrideHeaders
	 *            A list of headers to override those in the file or null to use the
	 *            headers from the file. If this is null and headerLineCount is set
	 *            to 0, an IllegalArgumentException ill be thrown.
	 * @param defaultValues
	 *            A list of default values to substitute during the summarise
	 *            process if there is no value given for the matching field in the
	 *            CSV file. The length of this list must either be 0 or the same as
	 *            the number of fields.
	 * @param headerLineCount
	 *            The number of header lines to expect
	 * @throws IOException
	 *             If there is an error reading or writing.
	 */
	public static void runSummarise(final Reader input, final CsvMapper inputMapper, final CsvSchema inputSchema,
			final Writer output, final Writer mappingOutput, final int maxSampleCount, final boolean showSampleCounts,
			final boolean debug, final List<String> overrideHeaders, final List<String> defaultValues,
			final int headerLineCount) throws IOException {
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

		final List<String> headers = parseForSummarise(input, inputMapper, inputSchema, emptyCounts, nonEmptyCounts,
				possibleIntegerFields, possibleDoubleFields, valueCounts, rowCount, overrideHeaders, headerLineCount,
				defaultValues);

		writeForSummarise(maxSampleCount, emptyCounts, nonEmptyCounts, possibleIntegerFields, possibleDoubleFields,
				valueCounts, headers, rowCount, showSampleCounts, output, mappingOutput);
	}

	/**
	 * Writes summary values and a stub mapping file based on the given
	 * {@link JDefaultDict}s.
	 * 
	 * @param maxSampleCount
	 *            The maximum number of samples to write out
	 * @param emptyCounts
	 *            A {@link JDefaultDict} containing the empty counts for each field
	 * @param nonEmptyCounts
	 *            A {@link JDefaultDict} containing the non-empty counts for each
	 *            field
	 * @param possibleIntegerFields
	 *            A {@link JDefaultDict} containing true if the field is possibly
	 *            integer and false otherwise
	 * @param possibleDoubleFields
	 *            A {@link JDefaultDict} containing true if the field is possibly
	 *            double and false otherwise
	 * @param valueCounts
	 *            A {@link JDefaultDict} containing values for each field and
	 *            attached counts
	 * @param headers
	 *            The headers that were either given or substituted
	 * @param rowCount
	 *            The total row count from the input file, used to determine if the
	 *            number of unique values matches the total number of rows
	 * @param showSampleCounts
	 *            True to attach sample counts to the sample output, and false to
	 *            omit it
	 * @param output
	 *            The {@link Writer} to contain the summarised statistics
	 * @param mappingOutput
	 *            The {@link Writer} to contain the stub mapping file
	 * @throws IOException
	 *             If there is an error writing
	 */
	static void writeForSummarise(final int maxSampleCount, final JDefaultDict<String, AtomicInteger> emptyCounts,
			final JDefaultDict<String, AtomicInteger> nonEmptyCounts,
			final JDefaultDict<String, AtomicBoolean> possibleIntegerFields,
			final JDefaultDict<String, AtomicBoolean> possibleDoubleFields,
			final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts, final List<String> headers,
			final AtomicInteger rowCount, final boolean showSampleCounts, final Writer output,
			final Writer mappingOutput) throws IOException {
		// This schema defines the fields and order for the columns in the
		// summary CSV file
		final CsvSchema summarySchema = getSummaryCsvSchema();
		final CsvSchema mappingSchema = getMappingCsvSchema();

		// Shared StringBuilder across fields for efficiency
		// After each field the StringBuilder is truncated
		final StringBuilder sharedSampleValueBuilder = new StringBuilder();
		final BiConsumer<? super String, ? super String> sampleHandler = (nextSample, nextCount) -> {
			if (sharedSampleValueBuilder.length() > 0) {
				sharedSampleValueBuilder.append(", ");
			}
			if (nextSample.length() > 200) {
				sharedSampleValueBuilder.append(nextSample.substring(0, 200));
				sharedSampleValueBuilder.append("...");
			} else {
				sharedSampleValueBuilder.append(nextSample);
			}
			if (showSampleCounts) {
				sharedSampleValueBuilder.append("(*" + nextCount + ")");
			}
		};
		try (final SequenceWriter csvWriter = CSVStream.newCSVWriter(output, summarySchema);
				final SequenceWriter mappingWriter = CSVStream.newCSVWriter(mappingOutput, mappingSchema);) {
			// Need to do this to get the header line written out in this case
			if (rowCount.get() == 0) {
				csvWriter.write(Arrays.asList());
				mappingWriter.write(Arrays.asList());
			}
			headers.forEach(nextHeader -> {
				try {
					final int emptyCount = emptyCounts.get(nextHeader).get();
					final int nonEmptyCount = nonEmptyCounts.get(nextHeader).get();
					JDefaultDict<String, AtomicInteger> nextValueCount = valueCounts.get(nextHeader);
					final int valueCount = nextValueCount.keySet().size();
					final boolean possiblePrimaryKey = valueCount == nonEmptyCount && valueCount == rowCount.get();

					boolean possiblyInteger = false;
					boolean possiblyDouble = false;
					// Only expose our numeric type guess if non-empty values
					// found
					// This is important, as it should default to true unless
					// evidence to the contrary is found, with the total number of observations,
					// when equal to 0, being used to identify the false positive cases
					if (nonEmptyCount > 0) {
						possiblyInteger = possibleIntegerFields.get(nextHeader).get();
						possiblyDouble = possibleDoubleFields.get(nextHeader).get();
					}

					final Stream<String> stream = nextValueCount.keySet().stream();
					if (maxSampleCount > 0) {
						stream.limit(maxSampleCount).sorted()
								.forEach(s -> sampleHandler.accept(s, nextValueCount.get(s).toString()));
						if (valueCount > maxSampleCount) {
							sharedSampleValueBuilder.append(", ...");
						}
					} else if (maxSampleCount < 0) {
						stream.sorted().forEach(s -> sampleHandler.accept(s, nextValueCount.get(s).toString()));
					}

					csvWriter.write(Arrays.asList(nextHeader, emptyCount, nonEmptyCount, valueCount, possiblePrimaryKey,
							possiblyInteger, possiblyDouble, sharedSampleValueBuilder));
					final String mappingFieldType = possiblyInteger ? "INTEGER" : possiblyDouble ? "DECIMAL" : "TEXT";
					mappingWriter.write(Arrays.asList(nextHeader, nextHeader, "",
							ValueMapping.ValueMappingLanguage.DBSCHEMA.name(), mappingFieldType));
				} catch (Exception e) {
					throw new RuntimeException(e);
				} finally {
					// Very important to reset this shared StringBuilder after
					// each row is written
					sharedSampleValueBuilder.setLength(0);
				}
			});
		}
	}

	/**
	 * Parse the given inputs to in-memory maps to allow for summarisation.
	 * 
	 * @param input
	 *            The {@link Reader} containing the inputs to be summarised.
	 * @param inputMapper
	 *            The CsvMapper to use to parse the file into memory
	 * @param inputSchema
	 *            The CsvSchema to use to help the mapper parse the file into memory
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
	 * @param overrideHeaders
	 *            Headers to use to override those in the file, or null to rely on
	 *            the headers from the file
	 * @param headerLineCount
	 *            The number of lines in the file that must be skipped, or 0 to not
	 *            skip any headers and instead use overrideHeaders
	 * @param defaultValues
	 *            A list that is either empty, signifying there are no default
	 *            values known, or exactly the same length as each row in the CSV
	 *            file being parsed. If the values for a field are empty/missing,
	 *            and a non-null, non-empty value appears in this list, it will be
	 *            substituted in when calculating the statistics.
	 * @return The list of headers that were either overridden or found in the file
	 * @throws IOException
	 *             If there is an error reading from the file
	 * @throws CSVStreamException
	 *             If there is a problem processing the CSV content
	 */
	static List<String> parseForSummarise(final Reader input, final CsvMapper inputMapper, final CsvSchema inputSchema,
			final JDefaultDict<String, AtomicInteger> emptyCounts,
			final JDefaultDict<String, AtomicInteger> nonEmptyCounts,
			final JDefaultDict<String, AtomicBoolean> possibleIntegerFields,
			final JDefaultDict<String, AtomicBoolean> possibleDoubleFields,
			final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts, final AtomicInteger rowCount,
			final List<String> overrideHeaders, final int headerLineCount, List<String> defaultValues)
			throws IOException, CSVStreamException {
		final long startTime = System.currentTimeMillis();
		final BiFunction<List<String>, List<String>, List<String>> summariseFunction = getSummaryFunctionWithStartTime(
				emptyCounts, nonEmptyCounts, possibleIntegerFields, possibleDoubleFields, valueCounts, rowCount,
				startTime);
		return parseForSummarise(input, inputMapper, inputSchema, overrideHeaders, headerLineCount, defaultValues,
				summariseFunction);
	}

	/**
	 * Returns a function that can be used as a summary function, using the given
	 * start time for timing analysis.
	 * 
	 * @param emptyCounts
	 *            A {@link JDefaultDict} used to store counts of non-empty fields,
	 *            based on {@link String#trim()} and {@link String#isEmpty()}.
	 * @param nonEmptyCounts
	 *            A {@link JDefaultDict} used to store counts of non-empty fields,
	 *            based on {@link String#trim()} and {@link String#isEmpty()}.
	 * @param possibleIntegerFields
	 *            A {@link JDefaultDict} used to store possible integer fields
	 * @param possibleDoubleFields
	 *            A {@link JDefaultDict} used to store possible double fields
	 * @param valueCounts
	 *            A {@link JDefaultDict} used to store value counts
	 * @param rowCount
	 *            The row count variable
	 * @param startTime
	 *            The start time reference, obtained using
	 *            {@link System#currentTimeMillis()}, for the timing analysis.
	 * @return A function which can be passed to
	 *         {@link #parseForSummarise(Reader, CsvMapper, CsvSchema, List, int, List, BiFunction)}
	 */
	public static BiFunction<List<String>, List<String>, List<String>> getSummaryFunctionWithStartTime(
			final JDefaultDict<String, AtomicInteger> emptyCounts,
			final JDefaultDict<String, AtomicInteger> nonEmptyCounts,
			final JDefaultDict<String, AtomicBoolean> possibleIntegerFields,
			final JDefaultDict<String, AtomicBoolean> possibleDoubleFields,
			final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts, final AtomicInteger rowCount,
			final long startTime) {
		return (h, l) -> {
			int nextLineNumber = rowCount.incrementAndGet();
			if (nextLineNumber % 10000 == 0) {
				double secondsSinceStart = (System.currentTimeMillis() - startTime) / 1000.0d;
				System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n", nextLineNumber,
						secondsSinceStart, nextLineNumber / secondsSinceStart);
			}
			for (int i = 0; i < h.size(); i++) {
				if (l.get(i).trim().isEmpty()) {
					emptyCounts.get(h.get(i)).incrementAndGet();
				} else {
					nonEmptyCounts.get(h.get(i)).incrementAndGet();
					valueCounts.get(h.get(i)).get(l.get(i)).incrementAndGet();
					try {
						Integer.parseInt(l.get(i));
					} catch (NumberFormatException nfe) {
						possibleIntegerFields.get(h.get(i)).set(false);
					}
					try {
						Double.parseDouble(l.get(i));
					} catch (NumberFormatException nfe) {
						possibleDoubleFields.get(h.get(i)).set(false);
					}
				}
			}
			return l;
		};
	}

	/**
	 * Parse the given inputs to in-memory maps to allow for summarisation.
	 * 
	 * @param input
	 *            The {@link Reader} containing the inputs to be summarised.
	 * @param inputMapper
	 *            The CsvMapper to use to parse the file into memory
	 * @param inputSchema
	 *            The CsvSchema to use to help the mapper parse the file into memory
	 * @param overrideHeaders
	 *            Headers to use to override those in the file, or null to rely on
	 *            the headers from the file
	 * @param headerLineCount
	 *            The number of lines in the file that must be skipped, or 0 to not
	 *            skip any headers and instead use overrideHeaders
	 * @param defaultValues
	 *            A list that is either empty, signifying there are no default
	 *            values known, or exactly the same length as each row in the CSV
	 *            file being parsed. If the values for a field are empty/missing,
	 *            and a non-null, non-empty value appears in this list, it will be
	 *            substituted in when calculating the statistics.
	 * @param summariseFunction
	 *            The function which will perform the summarisation
	 * @return The list of headers that were either overridden or found in the file
	 * @throws IOException
	 *             If there is an error reading from the file
	 * @throws CSVStreamException
	 *             If there is a problem processing the CSV content
	 */
	private static List<String> parseForSummarise(final Reader input, final CsvMapper inputMapper,
			final CsvSchema inputSchema, final List<String> overrideHeaders, final int headerLineCount,
			List<String> defaultValues, BiFunction<List<String>, List<String>, List<String>> summariseFunction)
			throws IOException, CSVStreamException {
		// This will be populated with whatever is recognised as the headers when the
		// input is parsed, so we can return it from this function
		final List<String> headers = new ArrayList<>();
		CSVStream.parse(input, h -> headers.addAll(h), summariseFunction, NULL_CONSUMER, overrideHeaders, defaultValues,
				headerLineCount, inputMapper, inputSchema);
		return headers;
	}

	/**
	 * @return A {@link CsvSchema} representing the fields in the summary results
	 *         file
	 */
	public static CsvSchema getSummaryCsvSchema() {
		final CsvSchema summarySchema = CsvSchema.builder().addColumn("fieldName")
				.addColumn("emptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("nonEmptyCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("uniqueValueCount", CsvSchema.ColumnType.NUMBER)
				.addColumn("possiblePrimaryKey", CsvSchema.ColumnType.BOOLEAN)
				.addColumn("possiblyInteger", CsvSchema.ColumnType.BOOLEAN)
				.addColumn("possiblyFloatingPoint", CsvSchema.ColumnType.BOOLEAN).addColumn("sampleValues")
				.setUseHeader(true).build();
		return summarySchema;
	}

	/**
	 * @return A {@link CsvSchema} representing the fields in the mapping file
	 */
	public static CsvSchema getMappingCsvSchema() {
		final CsvSchema mappingSchema = CsvSchema.builder()
				.addColumn(ValueMapping.OLD_FIELD, CsvSchema.ColumnType.STRING)
				.addColumn(ValueMapping.NEW_FIELD, CsvSchema.ColumnType.STRING)
				.addColumn(ValueMapping.SHOWN, CsvSchema.ColumnType.STRING)
				.addColumn(ValueMapping.DEFAULT, CsvSchema.ColumnType.STRING)
				.addColumn(ValueMapping.LANGUAGE, CsvSchema.ColumnType.STRING)
				.addColumn(ValueMapping.MAPPING, CsvSchema.ColumnType.STRING).setUseHeader(true).build();
		return mappingSchema;
	}

}
