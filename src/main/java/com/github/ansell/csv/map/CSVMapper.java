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
package com.github.ansell.csv.map;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.sum.CSVSummariser;
import com.github.ansell.csv.util.LineFilteredException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.csv.util.ValueMappingContext;
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
		final OptionSpec<File> overrideHeadersFile = parser.accepts("override-headers-file").withRequiredArg()
				.ofType(File.class).describedAs(
						"A file whose first line contains the headers to use, to override those found in the file.");
		final OptionSpec<Integer> headerLineCount = parser.accepts("header-line-count").withRequiredArg()
				.ofType(Integer.class)
				.describedAs(
						"The number of header lines present in the file. Can be used in conjunction with override-headers-file to substitute a different set of headers")
				.defaultsTo(1);
		final OptionSpec<Boolean> appendToExistingOption = parser.accepts("append-to-existing").withRequiredArg()
				.ofType(Boolean.class).describedAs("Append to an existing file").defaultsTo(false);
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

		final Path mappingPath = mapping.value(options).toPath();
		if (!Files.exists(mappingPath)) {
			throw new FileNotFoundException("Could not find mapping CSV file: " + mappingPath.toString());
		}

		boolean debugBoolean = debug.value(options);

		int headerLineCountInt = headerLineCount.value(options);

		// Defaults to null, with any strings in the file overriding that
		AtomicReference<List<String>> overrideHeadersList = new AtomicReference<>();
		if (options.has(overrideHeadersFile)) {
			CSVSummariser.parseOverrideHeaders(overrideHeadersFile, options, overrideHeadersList);
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

		// Double up for now on the append option, as we always want to write headers,
		// except when we are appending to an existing file, in which case we check that
		// the headers already exist
		boolean writeHeaders = !appendToExistingOption.value(options);

		OpenOption[] writeOptions = new OpenOption[1];
		// Append if needed, otherwise verify that the file is created from scratch
		writeOptions[0] = writeHeaders ? StandardOpenOption.CREATE_NEW : StandardOpenOption.APPEND;

		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);
				final BufferedReader readerInput = Files.newBufferedReader(inputPath);) {
			List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
			final List<String> outputHeaders = ValueMapping.getOutputFieldsFromList(map);

			Writer writer = null;
			List<String> defaultValues = Collections.emptyList();
			List<String> overrideHeaders = overrideHeadersList.get();
			try {
				if (options.has(output)) {
					// If we aren't planning on writing headers, we parse just the header line
					if (!writeHeaders) {
						CSVStream.parse(Files.newBufferedReader(output.value(options).toPath(), StandardCharsets.UTF_8),
								h -> {
									// Headers must match exactly with those we are planning to write out
									if (!outputHeaders.equals(h)) {
										throw new IllegalArgumentException(
												"Could not append to file as its existing headers did not match: existing=["
														+ h + "] new=[" + outputHeaders + "]");
									}
								}, (h, l) -> l, l -> {
								}, overrideHeaders, defaultValues, headerLineCountInt, inputMapper, inputSchema);

					}

					writer = Files.newBufferedWriter(output.value(options).toPath(), StandardCharsets.UTF_8,
							writeOptions);
				} else {
					writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
				}

				runMapper(readerInput, map, writer, writeHeaders, outputHeaders, overrideHeaders, headerLineCountInt,
						inputMapper, inputSchema);
			} finally {
				if (writer != null) {
					writer.close();
				}
			}
		}
	}

	private static void runMapper(Reader input, List<ValueMapping> map, Writer output, boolean writeHeaders,
			List<String> outputHeaders, List<String> overrideHeaders, int headerLineCount, CsvMapper inputMapper,
			CsvSchema inputSchema) throws ScriptException, IOException {

		final Map<String, String> defaultValues = ValueMapping.getDefaultValuesFromList(map);
		final CsvSchema schema = CSVStream.buildSchema(outputHeaders, writeHeaders);
		final Writer writer = output;

		try (final SequenceWriter csvWriter = CSVStream.newCSVWriter(writer, schema);) {
			final List<String> inputHeaders = new ArrayList<>();
			final List<String> previousLine = new ArrayList<>();
			final List<String> previousMappedLine = new ArrayList<>();
			final JDefaultDict<String, Set<String>> primaryKeys = new JDefaultDict<>(k -> new HashSet<>());
			final AtomicInteger lineNumber = new AtomicInteger(0);
			final AtomicInteger filteredLineNumber = new AtomicInteger(0);
			final long startTime = System.currentTimeMillis();
			final BiConsumer<List<String>, List<String>> mapLineConsumer = Unchecked.biConsumer((l, m) -> {
				previousLine.clear();
				previousLine.addAll(l);
				previousMappedLine.clear();
				previousMappedLine.addAll(m);
				csvWriter.write(m);
			});
			CSVStream.parse(input, h -> inputHeaders.addAll(h), (h, l) -> {
				final int nextLineNumber = lineNumber.incrementAndGet();
				if (nextLineNumber % 1000 == 0) {
					double secondsSinceStart = (System.currentTimeMillis() - startTime) / 1000.0d;
					System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n", nextLineNumber,
							secondsSinceStart, nextLineNumber / secondsSinceStart);
				}
				final int nextFilteredLineNumber = filteredLineNumber.incrementAndGet();
				try {
					List<String> mapLine = ValueMapping.mapLine(new ValueMappingContext(inputHeaders, l, previousLine,
							previousMappedLine, map, primaryKeys, nextLineNumber, nextFilteredLineNumber,
							mapLineConsumer, outputHeaders, defaultValues, Optional.empty()));
					mapLineConsumer.accept(l, mapLine);
				} catch (final LineFilteredException e) {
					// Swallow line filtered exception and return null below to
					// eliminate it
					// We expect streamCSV to operate in sequential order, print
					// a warning if it doesn't
					boolean success = filteredLineNumber.compareAndSet(nextFilteredLineNumber,
							nextFilteredLineNumber - 1);
					if (!success) {
						System.out.println("Line numbers may not be consistent");
					}
				}
				return null;
			}, l -> {
			}, overrideHeaders, Collections.emptyList(), headerLineCount, inputMapper, inputSchema);
		}
	}

}
