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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.JSONStream;
import com.github.ansell.csv.util.LineFilteredException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Maps from a JSON file to a CSV file based on the supplied mapping
 * definitions.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JSONMapper {

	/**
	 * Private constructor for static only class
	 */
	private JSONMapper() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input JSON file to be mapped.");
		final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg().ofType(File.class).required()
				.describedAs("The mapping file.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class)
				.describedAs("The mapped CSV file, or the console if not specified.");
		final OptionSpec<String> basePathOption = parser.accepts("base-path").withRequiredArg().ofType(String.class)
				.describedAs("The base path in the JSON document to locate the array of objects to be summarised")
				.defaultsTo("/");

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
			throw new FileNotFoundException("Could not find input JSONss file: " + inputPath.toString());
		}

		final Path mappingPath = mapping.value(options).toPath();
		if (!Files.exists(mappingPath)) {
			throw new FileNotFoundException("Could not find mapping CSV file: " + mappingPath.toString());
		}

		final Writer writer;
		if (options.has(output)) {
			writer = Files.newBufferedWriter(output.value(options).toPath());
		} else {
			writer = new BufferedWriter(new OutputStreamWriter(System.out));
		}

		JsonPointer basePath = JsonPointer.compile(basePathOption.value(options));

		ObjectMapper jsonMapper = new ObjectMapper();
		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);
				final BufferedReader readerInput = Files.newBufferedReader(inputPath);) {
			List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);

			runMapper(readerInput, map, writer, basePath, jsonMapper);
		} finally {
			writer.close();
		}
	}

	private static void runMapper(Reader input, List<ValueMapping> map, Writer output, JsonPointer basePath,
			ObjectMapper jsonMapper) throws ScriptException, IOException {

		final List<String> outputHeaders = ValueMapping.getOutputFieldsFromList(map);
		final Map<String, String> defaultValues = ValueMapping.getDefaultValuesFromList(map);
		final Map<String, JsonPointer> fieldRelativePaths = map.stream().collect(Collectors
				.toMap(ValueMapping::getOutputField, nextMapping -> JsonPointer.compile(nextMapping.getInputField())));
		final CsvSchema schema = CSVStream.buildSchema(outputHeaders);
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
			JSONStream.parse(input, h -> inputHeaders.addAll(h), (h, l) -> {
				final int nextLineNumber = lineNumber.incrementAndGet();
				if (nextLineNumber % 1000 == 0) {
					double secondsSinceStart = (System.currentTimeMillis() - startTime) / 1000.0d;
					System.out.printf("%d\tSeconds since start: %f\tRecords per second: %f%n", nextLineNumber,
							secondsSinceStart, nextLineNumber / secondsSinceStart);
				}
				final int nextFilteredLineNumber = filteredLineNumber.incrementAndGet();
				try {
					List<String> mapLine = ValueMapping.mapLine(inputHeaders, l, previousLine, previousMappedLine, map,
							primaryKeys, nextLineNumber, nextFilteredLineNumber, mapLineConsumer, outputHeaders,
							defaultValues);
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
			}, basePath, fieldRelativePaths, defaultValues, jsonMapper);
		}
	}

}
