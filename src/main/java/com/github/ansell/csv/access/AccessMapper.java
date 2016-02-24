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
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.JSONUtil;
import com.github.ansell.csv.util.ValueMapping;
import com.github.ansell.csv.util.ValueMapping.ValueMappingLanguage;
import com.github.ansell.jdefaultdict.JDefaultDict;
import com.github.jsonldjava.utils.JsonUtils;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
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

	private static final String DOT_REGEX = "\\.";
	private static final Pattern DOT_PATTERN = Pattern.compile(DOT_REGEX);
	private static final String COMMA_REGEX = "\\,";
	private static final Pattern COMMA_PATTERN = Pattern.compile(COMMA_REGEX);

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
		final OptionSpec<Boolean> debug = parser.accepts("debug").withOptionalArg().ofType(Boolean.class)
				.defaultsTo(Boolean.FALSE).describedAs("Set to true to debug the table structures");

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
			throw new FileNotFoundException("Could not find mapping CSV file: " + mappingPath.toString());
		}

		try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);) {
			List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
			try (final InputStream readerDB = Files.newInputStream(inputPath);) {
				dumpToCSVs(readerDB, output.value(options).toPath(), outputPrefix.value(options), debug.value(options));
			}
			// Read the database again to map it to a single CSV
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
			ConcurrentMap<String, ConcurrentMap<ValueMapping, Tuple2<Table, Table>>> foreignKeyMapping = new JDefaultDict<>(
					k -> new ConcurrentHashMap<>());
			ConcurrentMap<ValueMapping, Joiner> joiners = new ConcurrentHashMap<>();
			// Populate the table mapping for each value mapping
			Table originTable = parseTableMappings(map, db, foreignKeyMapping, joiners);
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
						writeNextRow(map, foreignKeyMapping, joiners, csvWriter, nextRow);
					}
				}
			}
		}
	}

	private static Table parseTableMappings(List<ValueMapping> map, final Database db,
			ConcurrentMap<String, ConcurrentMap<ValueMapping, Tuple2<Table, Table>>> foreignKeyMapping,
			ConcurrentMap<ValueMapping, Joiner> joiners) throws IOException {
		Table originTable = null;
		for (final ValueMapping nextValueMapping : map) {
			final String[] splitDBField = DOT_PATTERN.split(nextValueMapping.getInputField());
			System.out.println(nextValueMapping.getInputField());
			final Table nextTable = db.getTable(splitDBField[0]);
			if (originTable == null) {
				originTable = nextTable;
			}

			if (nextValueMapping.getLanguage() == ValueMappingLanguage.ACCESS) {
				final String[] splitForeignDBField = DOT_PATTERN.split(nextValueMapping.getMapping());
				final Table nextForeignTable = db.getTable(splitForeignDBField[0]);
				if (nextForeignTable == null) {
					throw new RuntimeException(
							"Could not find table referenced by access mapping: " + nextValueMapping.getMapping());
				}
				foreignKeyMapping.get(splitForeignDBField[0]).put(nextValueMapping,
						Tuple.tuple(nextTable, nextForeignTable));
				try {
					final Joiner create = Joiner.create(nextTable, nextForeignTable);
					if (create != null) {
						joiners.put(nextValueMapping, create);
						System.out.println("PK->FK: " + joiners.get(nextValueMapping).toFKString());
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				}
			}
		}
		return originTable;
	}

	private static void writeNextRow(List<ValueMapping> map,
			ConcurrentMap<String, ConcurrentMap<ValueMapping, Tuple2<Table, Table>>> foreignKeyMapping,
			ConcurrentMap<ValueMapping, Joiner> joiners, final SequenceWriter csvWriter, Row nextRow)
					throws IOException {
		// Rows, indexed by the table that they came from
		Map<String, Row> componentRowsForThisRow = new HashMap<>();
		for (final ValueMapping nextValueMapping : map) {
			String[] splitDBFieldSource = DOT_PATTERN.split(nextValueMapping.getInputField());
			if (nextValueMapping.getLanguage() == ValueMappingLanguage.ACCESS) {
				String[] splitDBFieldDest = DOT_PATTERN.split(nextValueMapping.getMapping());
				if (!componentRowsForThisRow.containsKey(splitDBFieldDest[0])) {
					// If we have a mapping to another table for the input
					// field, then use it
					if (foreignKeyMapping.containsKey(splitDBFieldDest[0])) {
						if (joiners.containsKey(nextValueMapping)) {
							getRowFromJoiner(joiners, componentRowsForThisRow, nextValueMapping, splitDBFieldSource,
									splitDBFieldDest);
						} else {
							getRowFromTables(foreignKeyMapping, componentRowsForThisRow, splitDBFieldDest);
						}
					}
				}
			} else {
				// Else we use the current table to populate the output rows
				if (!componentRowsForThisRow.containsKey(splitDBFieldSource[0])) {
					componentRowsForThisRow.put(splitDBFieldSource[0], nextRow);
				}
			}
		}

		// Populate the foreign row values
		Map<String, String> output = new HashMap<>();
		for (final ValueMapping nextValueMapping : map) {
			String[] splitDBField = DOT_PATTERN.split(nextValueMapping.getInputField());
			if (componentRowsForThisRow.containsKey(splitDBField[0])) {
				Row findFirstRow = componentRowsForThisRow.get(splitDBField[0]);
				Object nextColumnValue = findFirstRow.get(splitDBField[1]);
				if (nextColumnValue != null) {
					output.put(nextValueMapping.getOutputField(), nextColumnValue.toString());
				}
			}
		}

		List<String> nextEmittedRow = new ArrayList<>(map.size());
		// Then after all are filled, emit the row
		for (final ValueMapping nextValueMapping : map) {
			nextEmittedRow.add(output.getOrDefault(nextValueMapping.getOutputField(), ""));
		}
		csvWriter.write(nextEmittedRow);
	}

	private static void getRowFromTables(
			ConcurrentMap<String, ConcurrentMap<ValueMapping, Tuple2<Table, Table>>> foreignKeyMapping,
			Map<String, Row> componentRowsForThisRow, String[] splitDBFieldOutput) throws IOException {
		// A joiner could not be created for this case as the original database
		// did not setup an actual foreign key for the relationship
		ConcurrentMap<ValueMapping, Tuple2<Table, Table>> mapping = foreignKeyMapping.get(splitDBFieldOutput[0]);

		for (Entry<ValueMapping, Tuple2<Table, Table>> entry : mapping.entrySet()) {
			ValueMapping nextMapping = entry.getKey();
			Table origin = entry.getValue().v1();
			Table dest = entry.getValue().v2();

			Row originRow = componentRowsForThisRow.get(origin.getName());
			if (originRow == null) {
				System.out.println(
						"Could not find row: Maybe the order of the mapping file needs changing: " + nextMapping);
			}
			Map<String, Object> singletonMap = buildMatchMap(nextMapping, originRow);

			// Cursor cursor = dest.getDefaultCursor();

			// HACK: This will not work if they are not looking for the primary
			// key on the destination
			Cursor cursor = null;

			try {
				cursor = dest.getPrimaryKeyIndex().newCursor().toIndexCursor();
			} catch (IllegalArgumentException e) {
				cursor = null;
			}

			boolean findFirstRow = false;

			if (cursor != null) {
				findFirstRow = cursor.findFirstRow(singletonMap);
			}

			if (cursor != null && findFirstRow) {
				Row currentRow = cursor.getCurrentRow();
				componentRowsForThisRow.put(splitDBFieldOutput[0], currentRow);
			} else {
				// If the fast index cursor did not work fall back to the slow
				// default cursor
				Cursor slowCursor = dest.getDefaultCursor();

				boolean slowFindFirstRow = slowCursor.findFirstRow(singletonMap);

				if (slowFindFirstRow) {
					Row currentRow = slowCursor.getCurrentRow();
					componentRowsForThisRow.put(splitDBFieldOutput[0], currentRow);
				} else {
					// System.out.println("Could not match proposed foreign key
					// from " + nextMapping.getInputField()
					// + " to " + nextMapping.getMapping() + " (no joiner) based
					// on the key: " + nextFKValue);
				}
			}
		}
	}

	private static Map<String, Object> buildMatchMap(ValueMapping mapping, Row originRow) {
		Map<String, Object> result = new HashMap<>();

		//System.out.println("Building match map for: " + mapping);

		String[] destFields = COMMA_PATTERN.split(mapping.getMapping());
		String[] sourceFields = COMMA_PATTERN.split(mapping.getInputField());

		if (destFields.length != sourceFields.length) {
			throw new RuntimeException("Source and destination mapping fields must be equal size: " + mapping);
		}

		for (int i = 0; i < destFields.length; i++) {
			String[] destField = DOT_PATTERN.split(destFields[i]);
			String[] sourceField = DOT_PATTERN.split(sourceFields[i]);
			Object nextFKValue = originRow.get(sourceField[1]);
			result.put(destField[1], nextFKValue);
		}

		return result;
	}

	private static void getRowFromJoiner(ConcurrentMap<ValueMapping, Joiner> joiners,
			Map<String, Row> componentRowsForThisRow, final ValueMapping nextValueMapping, String[] splitDBField,
			String[] splitDBFieldOutput) throws IOException {
		String key = splitDBField[0];
		Row fromRow = componentRowsForThisRow.get(key);

		if (fromRow == null) {
			System.out.println("Could not find any linked rows with the key: " + key);
		} else {
			Row findFirstRow = joiners.get(nextValueMapping).findFirstRow(fromRow);
			if (findFirstRow != null) {
				componentRowsForThisRow.put(splitDBFieldOutput[0], findFirstRow);
			}
		}
	}

	private static void dumpToCSVs(InputStream input, Path outputDir, String csvPrefix, boolean debug)
			throws IOException {
		Path tempFile = Files.createTempFile("Source-accessdb", ".accdb");
		Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);

		final CsvSchema schema = CSVUtil.buildSchema(Arrays.asList("OldField", "NewField", "Language", "Mapping"));
		try (final Database db = DatabaseBuilder.open(tempFile.toFile());
				final Writer columnCsv = Files
						.newBufferedWriter(outputDir.resolve(csvPrefix + "AutoMapping-Columns.csv"));
				final SequenceWriter columnCsvWriter = CSVUtil.newCSVWriter(new BufferedWriter(columnCsv), schema);) {
			for (String tableName : db.getTableNames()) {
				Table table = db.getTable(tableName);

				if (debug) {
					debugTable(table, columnCsvWriter);
				}

				System.out.println("");
				String csvName = csvPrefix + tableName + ".csv";
				Path csvPath = outputDir.resolve(csvName);
				System.out.println("Converting " + tableName + " to CSV: " + csvPath.toAbsolutePath().toString());

				String[] tempArray = new String[table.getColumnCount()];
				int x = 0;
				for (Column nextColumn : table.getColumns()) {
					tempArray[x++] = nextColumn.getName();
				}

				final CsvSchema fullFileSchema = CSVUtil.buildSchema(Arrays.asList(tempArray));
				try (final Writer fullFileCsv = Files.newBufferedWriter(csvPath);
						final SequenceWriter fullFileCsvWriter = CSVUtil.newCSVWriter(new BufferedWriter(fullFileCsv),
								fullFileSchema);) {
					int rows = 0;
					for (Row nextRow : table) {
						int i = 0;
						for (Object nextValue : nextRow.values()) {
							tempArray[i++] = nextValue == null ? null : nextValue.toString();
						}
						fullFileCsvWriter.write(Arrays.asList(tempArray));
						rows++;
					}
					System.out.println("Converted " + rows + " rows from table " + tableName);
				}
				System.out.println("");
				System.out.println("----------------------------");
			}
		}
	}

	private static void debugTable(Table table, SequenceWriter columnCsv) throws IOException {

		System.out.println("\tTable columns for " + table.getName());

		try {
			for (Column nextColumn : table.getColumns()) {
				System.out.println("\t\t" + nextColumn.getName());
				columnCsv.write(Arrays.asList(table.getName() + "." + nextColumn.getName(),
						table.getName() + "." + nextColumn.getName(), "", ""));
			}

			Index primaryKeyIndex = table.getPrimaryKeyIndex();
			System.out.println(
					"\tFound primary key index for table: " + table.getName() + " named " + primaryKeyIndex.getName());
			debugIndex(primaryKeyIndex, new HashSet<>(), columnCsv);

			for (Index nextIndex : table.getIndexes()) {
				if (!nextIndex.getName().equals(primaryKeyIndex.getName())) {
					System.out.println("\tFound non-primary key index for table: " + table.getName() + " named "
							+ nextIndex.getName());
					debugIndex(nextIndex, new HashSet<>(), null);
				}
			}
		} catch (IllegalArgumentException e) {
			System.out.println("No primary key index found for table: " + table.getName());
		}

		Cursor cursor = table.getDefaultCursor();
		int i = 0;
		while (cursor.moveToNextRow()) {
			if (i >= 5) {
				break;
			}
			System.out.println(cursor.getCurrentRow().toString());
			i++;
		}
	}

	private static void debugIndex(Index index, Set<Index> visited, SequenceWriter csvWriter) throws IOException {
		visited.add(index);
		System.out.println("\t\tIndex columns:");
		StringBuilder columnList = new StringBuilder();
		for (Index.Column nextColumn : index.getColumns()) {
			System.out.print("\t\t\t" + nextColumn.getName());

			if (columnList.length() > 0) {
				columnList.append(",");
			}
			columnList.append(index.getTable().getName() + "." + nextColumn.getName());
		}
		if (csvWriter != null) {
			csvWriter.write(
					Arrays.asList(columnList.toString(), columnList.toString(), "Access", columnList.toString()));
		}

		System.out.println("");
		Index referencedIndex = index.getReferencedIndex();
		if (referencedIndex != null) {
			System.out.println("\t" + index.getName() + " references another index: " + referencedIndex.getName());
			if (!visited.contains(referencedIndex)) {
				visited.add(referencedIndex);
				debugIndex(referencedIndex, visited, null);
			}

		}

	}

}
