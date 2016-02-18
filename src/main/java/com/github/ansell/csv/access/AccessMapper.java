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
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.commons.io.IOUtils;
import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.Joiner;

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

		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);) {
			List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
			try (final InputStream readerDB = Files.newInputStream(inputPath);) {
				dumpToCSVs(readerDB, output.value(options).toPath(), outputPrefix.value(options));
			}
			try (final InputStream readerDB = Files.newInputStream(inputPath);) {
				mapDBToSingleCSV(readerDB, map, output.value(options).toPath(),
						outputPrefix.value(options) + "Single-");
			}
		}
	}

	private static void mapDBToSingleCSV(InputStream readerDB, List<ValueMapping> map, Path csvPath, String csvPrefix)
			throws IOException {
		Path tempFile = Files.createTempFile("Source-accessdb", ".accdb");
		Files.copy(readerDB, tempFile, StandardCopyOption.REPLACE_EXISTING);

		try (final Database db = DatabaseBuilder.open(tempFile.toFile());) {
			// Ordered mappings so that the first table in the mapping is the
			// one to perform the base joins on
			Table originTable = null;
			ConcurrentMap<ValueMapping, Table> tableMapping = new ConcurrentHashMap<>();
			ConcurrentMap<ValueMapping, Table> foreignKeyMapping = new ConcurrentHashMap<>();
			ConcurrentMap<ValueMapping, Joiner> joiners = new ConcurrentHashMap<>();
			// Populate the table mapping for each value mapping
			for (final ValueMapping nextValueMapping : map) {
				String[] splitDBField = nextValueMapping.getInputField().split("\\.");
				System.out.println(nextValueMapping.getInputField());
				Table nextTable = db.getTable(splitDBField[0]);
				tableMapping.put(nextValueMapping, nextTable);
				if (originTable == null) {
					originTable = nextTable;
				}

				if (nextValueMapping.getLanguage() == ValueMappingLanguage.ACCESS) {
					String[] splitForeignDBField = nextValueMapping.getOutputField().split("\\.");
					foreignKeyMapping.put(nextValueMapping, db.getTable(splitForeignDBField[0]));
					try {
						joiners.put(nextValueMapping, Joiner.create(nextTable, db.getTable(splitForeignDBField[0])));
						System.out.println("PK->FK: " + joiners.get(nextValueMapping).toFKString());
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					}
				}
			}
			// There may have been no mappings...
			if (originTable != null) {
				List<String> headers = map.stream().map(m -> m.getOutputField()).collect(Collectors.toList());
				final CsvSchema schema = CSVUtil.buildSchema(headers);

				try (final Writer csv = Files
						.newBufferedWriter(csvPath.resolve(csvPrefix + originTable.getName() + ".csv"));
						final SequenceWriter csvWriter = CSVUtil.newCSVWriter(new BufferedWriter(csv), schema);) {
					// Run through the fields on the origin table joining them
					// as necessary before running the other non-access mappings
					// on the resulting list of strings
					for (Row nextRow : originTable) {
						ConcurrentMap<String, String> output = new ConcurrentHashMap<>();
						ConcurrentMap<String, Row> foreignRowsForThisRow = new ConcurrentHashMap<>();
						List<? extends Column> originColumns = originTable.getColumns();
						for (Column nextOriginColumn : originColumns) {
							for (final ValueMapping nextValueMapping : map) {
								String[] splitDBField = nextValueMapping.getInputField().split("\\.");
								if (splitDBField[0].equals(originTable.getName())
										&& splitDBField[1].equals(nextOriginColumn.getName())) {
									if (foreignKeyMapping.containsKey(nextValueMapping)) {
										if (joiners.containsKey(nextValueMapping)) {
											Row findFirstRow = joiners.get(nextValueMapping).findFirstRow(nextRow);
											if (findFirstRow != null) {
												foreignRowsForThisRow.put(splitDBField[0], findFirstRow);
											}
											// for (Map<String, Object> row :
											// joiners.get(nextValueMapping).getToTable())
											// {
											// System.out.println(row);
											// }
										} else {
											// System.out.println(
											// "TODO: Support fetching of
											// foreign keys when an index was
											// not available: "
											// +
											// nextValueMapping.getInputField()
											// + "=>"
											// +
											// nextValueMapping.getOutputField());
										}
									} else {
										Object nextColumnValue = nextRow.get(splitDBField[1]);
										if (nextColumnValue != null) {
											output.put(nextValueMapping.getOutputField(), nextColumnValue.toString());
										}
									}
								}
							}
						}

						// Populate the foreign row values
						for (final ValueMapping nextValueMapping : map) {
							if (foreignKeyMapping.containsKey(nextValueMapping)) {
								String[] splitDBField = nextValueMapping.getInputField().split("\\.");
								Row findFirstRow = foreignRowsForThisRow.get(splitDBField[0]);
								Object nextColumnValue = findFirstRow.get(splitDBField[1]);
								if (nextColumnValue != null) {
									output.put(nextValueMapping.getOutputField(), nextColumnValue.toString());
								}
							}
						}

						List<String> nextEmittedRow = new ArrayList<>(map.size());
						// Then after all are filled, emit the row
						for (final ValueMapping nextValueMapping : map) {
							nextEmittedRow.add(output.getOrDefault(nextValueMapping.getOutputField(), "Unknown"));
						}
						// System.out.println("nextEmittedRow: " +
						// nextEmittedRow);
						csvWriter.write(nextEmittedRow);
					}
				}
			}
		}
	}

	private static void dumpToCSVs(InputStream input, Path outputDir, String csvPrefix) throws IOException {
		Path tempFile = Files.createTempFile("Source-accessdb", ".accdb");
		Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);

		try (final Database db = DatabaseBuilder.open(tempFile.toFile());) {
			for (String tableName : db.getTableNames()) {
				System.out.println("");
				String csvName = csvPrefix + tableName + ".csv";
				Path csvPath = outputDir.resolve(csvName);
				System.out.println("Converting " + tableName + " to CSV: " + csvPath.toAbsolutePath().toString());
				Table table = db.getTable(tableName);

				debugTable(table);

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
				System.out.println("");
				System.out.println("----------------------------");
			}
		}
	}

	private static void debugTable(Table table) throws IOException {

		System.out.println("\tTable columns for " + table.getName());

		for (Column nextColumn : table.getColumns()) {
			System.out.println("\t\t" + nextColumn.getName());
		}

		try {
			Index primaryKeyIndex = table.getPrimaryKeyIndex();
			System.out.println(
					"\tFound primary key index for table: " + table.getName() + " named " + primaryKeyIndex.getName());
			debugIndex(primaryKeyIndex, new HashSet<>());

			for (Index nextIndex : table.getIndexes()) {
				if (!nextIndex.getName().equals(primaryKeyIndex.getName())) {
					System.out.println("\tFound non-primary key index for table: " + table.getName() + " named "
							+ nextIndex.getName());
					debugIndex(nextIndex, new HashSet<>());
				}
			}
		} catch (IllegalArgumentException e) {
			System.out.println("No primary key index found for table: " + table.getName());
		}

		Cursor cursor = table.getDefaultCursor();
		int i = 0;
		while (cursor.moveToNextRow()) {
			if (i >= 20) {
				break;
			}
			System.out.println(cursor.getCurrentRow().toString());
			i++;
		}
	}

	private static void debugIndex(Index index, Set<Index> visited) throws IOException {
		visited.add(index);
		System.out.println("\t\tIndex columns:");
		for (Index.Column nextColumn : index.getColumns()) {
			System.out.print("\t\t\t" + nextColumn.getName());
		}

		System.out.println("");
		Index referencedIndex = index.getReferencedIndex();
		if (referencedIndex != null) {
			System.out.println("\t" + index.getName() + " references another index: " + referencedIndex.getName());
			if (!visited.contains(referencedIndex)) {
				visited.add(referencedIndex);
				debugIndex(referencedIndex, visited);
			}

		}

	}

}
