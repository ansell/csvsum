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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.util.LineFilteredException;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.jdefaultdict.JDefaultDict;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Rewrites newlines that occur naturally in RFC4180 CSV files with replacement
 * characters so large CSV files can be sorted using traditional on-disk sorting
 * algorithms that rely solely on line ending characters.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVSorter {

	/**
	 * Private constructor for static only class
	 */
	private CSVSorter() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be mapped.");
		final OptionSpec<Boolean> rewrite = parser.accepts("rewrite").withRequiredArg().ofType(Boolean.class).required()
				.describedAs(
						"True to rewrite new lines to replacement characters and false to rewrite replacement characters to newlines.");
		final OptionSpec<String> replacementString = parser.accepts("replacement").withRequiredArg()
				.ofType(String.class).required().describedAs("The string used as a replacement for newlines.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class).required()
				.describedAs("The output sorted CSV file.");

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

		final Path outputPath = output.value(options).toPath();
		if (Files.exists(outputPath)) {
			throw new FileAlreadyExistsException("Output file already exists: " + outputPath.toString());
		}

		if (!Files.exists(outputPath.getParent())) {
			throw new FileNotFoundException("Could not find directory for output file: " + outputPath.getParent());
		}

		try (final BufferedReader readerInput = Files.newBufferedReader(inputPath);) {
			runRewriter(readerInput, rewrite.value(options), replacementString.value(options), outputPath,
					CSVStream.defaultMapper(), CSVStream.defaultSchema());
		}
	}

	public static void runRewriter(Reader input, boolean rewrite, String replacementString, Path output,
			CsvMapper mapper, CsvSchema schema) throws ScriptException, IOException {

		// Ignore headers as they are not compatible with sort algorithms, add
		// them back on after sorting
		CsvSchema firstWriteSchema = CsvSchema.builder().setUseHeader(false).build();

		List<String> inputHeaders = new ArrayList<>();

		Path tempDir = Files.createTempDirectory(output.getParent(), "temp-csvsort");
		Path tempFile = Files.createTempFile(tempDir, "temp-output", ".csv");

		try (final Writer tempOutput = Files.newBufferedWriter(tempFile);
				final SequenceWriter csvWriter = CSVStream.newCSVWriter(tempOutput, firstWriteSchema);) {
			final Consumer<List<String>> mapLineConsumer = Unchecked.consumer(l -> csvWriter.write(l));

			// Rewrite new lines in fields using the users choice of replacement
			CSVStream.parse(input, h -> inputHeaders.addAll(h), (h, l) -> {
				return l.stream().map(s -> s.replaceAll("\r\n", replacementString).replaceAll("\n", replacementString))
						.collect(Collectors.toList());
			}, mapLineConsumer);
		}
	}
}
