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
package com.github.ansell.csv.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.lambda.Unchecked;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.github.ansell.csv.util.CSVUtil;
import com.github.ansell.csv.util.ValueMapping;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Uploads from a CSV file to a database.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class CSVUpload {

	/**
	 * Private constructor for static only class
	 */
	private CSVUpload() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input CSV file to be mapped.");
		final OptionSpec<String> database = parser.accepts("database").withRequiredArg().ofType(String.class).required()
				.describedAs("The JDBC connection string for the database to upload to.");
		final OptionSpec<String> table = parser.accepts("table").withRequiredArg().ofType(String.class).required()
				.describedAs("The database table to upload to.");
		final OptionSpec<Boolean> dropTable = parser.accepts("drop-existing-table").withRequiredArg()
				.ofType(Boolean.class).defaultsTo(Boolean.FALSE)
				.describedAs("True to drop an existing table with this name and false otherwise.");
		final OptionSpec<Boolean> debug = parser.accepts("debug").withRequiredArg().ofType(Boolean.class)
				.defaultsTo(Boolean.FALSE).describedAs("True to debug and false otherwise.");
		final OptionSpec<File> mapping = parser.accepts("mapping").withRequiredArg().ofType(File.class).required()
				.describedAs("The mapping file.");

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

		final String databaseConnectionString = database.value(options);
		final String tableString = table.value(options);
		final Boolean dropTableBoolean = dropTable.value(options);
		final Boolean debugBoolean = debug.value(options);

		try (final Connection conn = DriverManager.getConnection(databaseConnectionString);) {
			if (dropTableBoolean) {
				dropExistingTable(tableString, conn);
			}
			try (final BufferedReader readerMapping = Files.newBufferedReader(mappingPath);) {
				List<ValueMapping> map = ValueMapping.extractMappings(readerMapping);
				conn.setAutoCommit(false);
				try (final Reader inputReader = Files.newBufferedReader(inputPath);) {
					upload(tableString, map, inputReader, conn);
				}
				conn.commit();
			}
		}
		if (debugBoolean) {
			try (final Connection conn = DriverManager.getConnection(databaseConnectionString);
					final Writer output = new PrintWriter(System.out);) {
				dumpTable(tableString, output, conn);
			}
		}
	}

	static void dropExistingTable(String tableString, Connection conn) {
		try (final Statement stmt = conn.createStatement();) {
			stmt.executeUpdate("DROP TABLE \"" + tableString + "\" ;");
		} catch (SQLException e) {
			// Silent to be a substitute for DROP TABLE IF EXISTS that not all
			// SQL databases support
		}
	}

	static void createTable(String tableName, List<String> h, List<String> types, StringBuilder insertStmt,
			Connection conn) throws SQLException {
		final StringBuilder createStmt = new StringBuilder();
		createStmt.append("CREATE TABLE \"").append(tableName).append("\" ( \n    ");
		insertStmt.append("INSERT INTO \"").append(tableName).append("\" ( \n    ");

		for (int i = 0; i < h.size(); i++) {
			if (i > 0) {
				createStmt.append(", ");
				insertStmt.append(", ");
			}
			String nextType = types.get(i);
			// HACK: Get this working for now, need to make types actually map
			// to the data
			if (h.get(i).equals("region_id")) {
				nextType = "TEXT";
			}
			createStmt.append("\"").append(h.get(i)).append("\" ").append(nextType).append(" ");
			insertStmt.append("\"").append(h.get(i)).append("\" ");
		}
		createStmt.append("\n)");

		insertStmt.append("\n)");

		insertStmt.append("\nVALUES ( \n    ");
		for (int i = 0; i < h.size(); i++) {
			if (i > 0) {
				insertStmt.append(", ");
			}
			insertStmt.append("?");
		}
		insertStmt.append("\n)");
		insertStmt.trimToSize();

		String createStatement = createStmt.toString();
		System.out.println(createStatement);

		try (final Statement stmt = conn.createStatement();) {
			stmt.executeUpdate(createStatement);
		}
	}

	static void dumpTable(String tableName, Writer output, Connection conn) throws IOException, SQLException {
		final String sql = "SELECT * FROM \"" + tableName + "\"";
		try (final Statement dumpStatement = conn.createStatement();
				final ResultSet results = dumpStatement.executeQuery(sql);) {
			final ResultSetMetaData metadata = results.getMetaData();
			final int columnCount = metadata.getColumnCount();
			final List<String> columnNames = new ArrayList<>(columnCount);
			for (int i = 1; i <= columnCount; i++) {
				columnNames.add(metadata.getColumnLabel(i));
			}
			final SequenceWriter csvWriter = CSVUtil.newCSVWriter(output, columnNames);
			final List<String> nextResult = new ArrayList<>(columnCount);
			while (results.next()) {
				for (int i = 1; i <= columnCount; i++) {
					nextResult.add(i - 1, results.getString(i));
				}
				csvWriter.write(nextResult);
				nextResult.clear();
			}
		}
	}

	static void upload(String tableName, List<ValueMapping> map, Reader input, Connection conn)
			throws IOException, SQLException {
		final AtomicReference<PreparedStatement> preparedStmt = new AtomicReference<>();
		try {
			final List<String> types = new ArrayList<>();
			final List<String> outputFieldNames = new ArrayList<>();
			CSVUtil.streamCSV(input, Unchecked.consumer(h -> {
				final StringBuilder insertStatement = new StringBuilder(2048);
				h.forEach(nextH -> {
					Optional<ValueMapping> firstMapping = map.stream()
							.filter(m -> m.getLanguage() == ValueMapping.ValueMappingLanguage.DBSCHEMA)
							.filter(m -> m.getInputField().equalsIgnoreCase(nextH)).findFirst();
					if (firstMapping.isPresent()) {
						types.add(firstMapping.get().getMapping());
						outputFieldNames.add(firstMapping.get().getOutputField());
					} else {
						types.add("TEXT");
						outputFieldNames.add(nextH);
					}
				});
				createTable(tableName, outputFieldNames, types, insertStatement, conn);
				String insertStatementString = insertStatement.toString();
				System.out.println(insertStatementString);
				preparedStmt.set(conn.prepareStatement(insertStatementString));
			}), Unchecked.biFunction((h, l) -> {
				uploadLine(outputFieldNames, l, types, preparedStmt.get());
				return l;
			}), l -> {
			});
		} finally {
			PreparedStatement closeable = preparedStmt.get();
			if (closeable != null) {
				closeable.close();
			}
			preparedStmt.set(null);
		}
	}

	static void uploadLine(List<String> h, List<String> l, List<String> types, PreparedStatement stmt)
			throws SQLException {
		for (int i = 0; i < h.size(); i++) {
			try {
				// HACK: Trying to be flexible here, but not sure if SQL will
				// allow flexibility anyway, so not sure if necessary
				if (types.get(i).equalsIgnoreCase("INTEGER")) {
					stmt.setInt(i + 1, Integer.parseInt(l.get(i)));
					continue;
				} else if (types.get(i).equalsIgnoreCase("DOUBLE")) {
					stmt.setDouble(i + 1, Double.parseDouble(l.get(i)));
					continue;
				}
			} catch (NumberFormatException e) {
			}
			stmt.setString(i + 1, l.get(i));
		}
		stmt.executeUpdate();
	}
}
