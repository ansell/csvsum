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
package com.github.ansell.csv.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.std.TextFileSorter;
import com.github.ansell.csv.stream.CSVStream;

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
public final class CSVSorter extends Sorter<List<String>> {

	public CSVSorter(SortConfig config, CsvMapper mapper, CsvSchema schema,
			Comparator<List<String>> stringListComparator) {
		super(config, new CSVSorterReaderFactory(mapper.copy(), schema), new CSVSorterWriterFactory(mapper.copy(), schema),
				stringListComparator);
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be mapped.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class).required()
				.describedAs("The output sorted CSV file.");
		final OptionSpec<Integer> idFieldIndex = parser.accepts("id-field-index").withRequiredArg()
				.ofType(Integer.class).required()
				.describedAs("The index of the column in the CSV file that is to be used for sorting");

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

		final int idFieldIndexInteger = idFieldIndex.value(options);

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
			runSorter(readerInput, outputPath, CSVStream.defaultMapper(), CSVStream.defaultSchema(),
					getComparator(idFieldIndexInteger));
		}
	}

	public static Comparator<List<String>> getComparator(int idFieldIndex) {
		return Comparator.comparing(list -> list.get(idFieldIndex));
	}

	public static void runSorter(Reader input, Path output, CsvMapper mapper, CsvSchema schema,
			Comparator<List<String>> comparator) throws ScriptException, IOException {

		Path tempDir = Files.createTempDirectory(output.getParent(), "temp-csvsort");
		Path tempFile = Files.createTempFile(tempDir, "temp-input", ".csv");

		try (final Writer tempWriter = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8);) {
			IOUtils.copy(input, tempWriter);
		}

		SortConfig sortConfig = new SortConfig().withMaxMemoryUsage(20 * 1000 * 1000)
				.withTempFileProvider(() -> Files.createTempFile(tempDir, "temp-intermediate-", ".csv").toFile());

		try (final InputStream tempInput = Files.newInputStream(tempFile);
				final OutputStream outputStream = Files.newOutputStream(output);
				final CSVSorter sorter = new CSVSorter(sortConfig, mapper, schema, comparator);) {
			sorter.sort(tempInput, outputStream);
		} finally {
			Files.walk(tempDir).sorted(Comparator.reverseOrder())
					// .map(Path::toFile)
					// .peek(System.out::println)
					// .forEach(File::delete);
					.forEach(f -> System.out.println("Would have deleted temporaryFile: " + f.toString()));
		}
	}
}
