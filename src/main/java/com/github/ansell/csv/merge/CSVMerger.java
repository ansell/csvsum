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
package com.github.ansell.csv.merge;

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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.LineFilteredException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Merges lines from one CSV file into another based on the supplied mapping
 * definitions.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVMerger {

	/**
	 * Private constructor for static only class
	 */
	private CSVMerger() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be mapped.");
		final OptionSpec<File> otherInput = parser.accepts("otherInput").withRequiredArg().ofType(File.class).required()
				.describedAs("The other input CSV file to be merged.");
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

		final Path otherInputPath = otherInput.value(options).toPath();
		if (!Files.exists(otherInputPath)) {
			throw new FileNotFoundException("Could not find other input CSV file: " + otherInputPath.toString());
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

		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);
				final BufferedReader readerInput = Files.newBufferedReader(inputPath);
				final BufferedReader readerOtherInput = Files.newBufferedReader(otherInputPath);) {
			List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
			runMapper(readerInput, readerOtherInput, map, writer);
		} finally {
			writer.close();
		}
	}

	private static void runMapper(Reader input, Reader otherInput, List<ValueMapping> map, Writer output)
			throws ScriptException, IOException {

		Function<ValueMapping, String> outputFields = e -> e.getOutputField();

		List<String> outputHeaders = map.stream().filter(k -> k.getShown()).map(outputFields)
				.collect(Collectors.toList());

		List<ValueMapping> mergeFieldsOrdered = map.stream()
				.filter(k -> k.getLanguage() == ValueMappingLanguage.CSVMERGE).collect(Collectors.toList());

		Map<String, List<ValueMapping>> mergeFields = map.stream()
				.filter(k -> k.getLanguage() == ValueMappingLanguage.CSVMERGE)
				.collect(Collectors.groupingBy(k -> k.getInputField()));

		final CsvSchema schema = CSVUtil.buildSchema(outputHeaders);

		try (final SequenceWriter csvWriter = CSVUtil.newCSVWriter(output, schema);) {
			List<String> inputHeaders = new ArrayList<>();
			CSVUtil.streamCSV(input, h -> inputHeaders.addAll(h), (h, l) -> {
				try {

					List<String> mergedInputHeaders = new ArrayList<>(inputHeaders);
					List<String> nextMergedLine = new ArrayList<>(l);

					otherInput.mark(Integer.MAX_VALUE);
					CSVUtil.streamCSV(otherInput, otherH -> {
					} , (otherH, otherL) -> {
						mergeFieldsOrdered.forEach(m -> {
							for (String inputHeader : inputHeaders) {
								if (mergeFields.containsKey(inputHeader)) {
									if (otherH.contains(m.getMapping())) {
										String otherValue = otherL.get(otherH.indexOf(m.getMapping()));
										if (l.get(inputHeaders.indexOf(m.getInputField())).equals(otherValue)) {
											mergedInputHeaders.addAll(otherH);
											nextMergedLine.addAll(otherL);
										}
									}
								}
							}
						});
						return otherL;
					} , otherL -> {

					});

					return ValueMapping.mapLine(mergedInputHeaders, nextMergedLine, map);
				} catch (final LineFilteredException e) {
					// Swallow line filtered exception and return null below to
					// eliminate it
				} catch (final IOException e) {
					throw new RuntimeException(e);
				} finally {
					try {
						otherInput.reset();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return null;
			} , Unchecked.consumer(l -> csvWriter.write(l)));

		}
	}

}
