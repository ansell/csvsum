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
package com.github.ansell.csv.access;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;
import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.ValueMapping;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Mapper from Access to CSV files.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class AccessMapper {

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input Access file to be mapped.");
		final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg().ofType(File.class).required()
				.describedAs("The mapping file.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(File.class).required()
				.describedAs("The directory to contain the mapped file.");
		final OptionSpec<String> outputPrefix = parser.accepts("prefix").withRequiredArg().ofType(String.class)
				.defaultsTo("Mapped-").describedAs("The prefix to use to name the mapped files.");

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
			throw new FileNotFoundException("Could not find input Access file: " + inputPath.toString());
		}

		final Path mappingPath = mapping.value(options).toPath();
		if (!Files.exists(mappingPath)) {
			throw new FileNotFoundException("Could not find mappng CSV file: " + mappingPath.toString());
		}

		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);
				final InputStream readerInput = Files.newInputStream(inputPath);) {
			List<ValueMapping> map = extractMappings(readerMapping);
			runMapper(readerInput, map, output.value(options).toPath(), outputPrefix.value(options));
		}
	}

	private static List<ValueMapping> extractMappings(Reader input) throws IOException {
		List<ValueMapping> result = new ArrayList<>();

		List<String> headers = new ArrayList<>();

		CSVUtil.streamCSV(input, h -> headers.addAll(h), (h, l) -> {
			return ValueMapping.newMapping(l.get(h.indexOf(ValueMapping.LANGUAGE)),
					l.get(h.indexOf(ValueMapping.OLD_FIELD)), l.get(h.indexOf(ValueMapping.NEW_FIELD)),
					l.get(h.indexOf(ValueMapping.MAPPING)));
		} , l -> result.add(l));

		return result;
	}

	private static void runMapper(InputStream input, List<ValueMapping> map, Path outputDir, String csvPrefix)
			throws ScriptException, IOException {
		Path tempFile = Files.createTempFile("Source-accessdb", ".accdb");
		Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);

		try (final Database db = DatabaseBuilder.open(tempFile.toFile());) {
			for (String tableName : db.getTableNames()) {
				String csvName = csvPrefix + tableName + ".csv";
				Path csvPath = outputDir.resolve(csvName);
				System.out.println("Converting " + tableName + " to CSV: " + csvPath.toAbsolutePath().toString());
				Table table = db.getTable(tableName);

				try {
					Index primaryKeyIndex = table.getPrimaryKeyIndex();
					System.out.println(
							"Found primary key index for table: " + tableName + " named " + primaryKeyIndex.getName());

					for (Index nextIndex : table.getIndexes()) {
						if (!nextIndex.getName().equals(primaryKeyIndex.getName())) {
							System.out.println("Found non-primary key index for table: " + tableName + " named "
									+ nextIndex.getName());
						}
					}
				} catch (IllegalArgumentException e) {
					System.out.println("No primary key index found for table: " + tableName);
				}

				String[] tempArray = new String[table.getColumnCount()];
				int x = 0;
				for (Column nextColumn : table.getColumns()) {
					tempArray[x++] = nextColumn.getName();
				}

				final CsvSchema schema = CSVUtil.buildSchema(Arrays.asList(tempArray));
				try (final Writer csv = Files.newBufferedWriter(csvPath);
						final SequenceWriter csvWriter = CSVUtil.newCSVWriter(new BufferedWriter(csv), schema);) {
					int rows = 0;
					for (Row nextRow : table) {
						int i = 0;
						for (Object nextValue : nextRow.values()) {
							tempArray[i++] = nextValue == null ? null : nextValue.toString();
						}
						csvWriter.write(Arrays.asList(tempArray));
						rows++;
					}
					System.out.println("Converted " + rows + " rows from table " + tableName);
				}
			}
		}
	}

}
