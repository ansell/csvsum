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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.jooq.lambda.Unchecked;

import com.github.ansell.csv.util.CSVUtil;

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

		final String databaseConnectionString = database.value(options);
		final String tableString = table.value(options);
		final Boolean dropTableBoolean = dropTable.value(options);

		try (final Connection conn = DriverManager.getConnection(databaseConnectionString);) {
			conn.setAutoCommit(false);
			if (dropTableBoolean) {
				dropExistingTable(conn, tableString);
			}
			try (final Reader inputReader = Files.newBufferedReader(inputPath);) {
				upload(inputReader, conn);
			}
			conn.commit();
		}
	}

	private static void dropExistingTable(Connection conn, String tableString) {
		throw new UnsupportedOperationException("TODO: Implement dropExistingTable!");
	}

	private static void upload(Reader input, Connection conn) throws IOException, SQLException {
		CSVUtil.streamCSV(input, h -> {
		}, Unchecked.biFunction((h, l) -> {
			return l;
		}), l -> {
		});

		throw new UnsupportedOperationException("TODO: Implement upload!");
	}

}
