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
package com.github.ansell.csv.sum;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csv.sum.JSONSummariser;

import joptsimple.OptionException;

/**
 * Tests for {@link JSONSummariser}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONSummariserTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.JSONSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		JSONSummariser.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.JSONSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		JSONSummariser.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.JSONSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();
		Path testFields = tempDir.newFile("test-array-single-entry.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-array-single-entry.csv"),
				testFields, StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(FileNotFoundException.class);
		JSONSummariser.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--fields",
				testFields.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.JSONSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMappingFileExists() throws Exception {
		Path testFile = tempDir.newFile("test-single-header.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-single-header.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
		Path testFields = tempDir.newFile("test-array-single-entry.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-array-single-entry.csv"),
				testFields, StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(FileNotFoundException.class);
		JSONSummariser.main("--input", testFile.toAbsolutePath().toString(), "--output-mapping",
				testFile.toAbsolutePath().toString(), "--fields", testFields.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.JSONSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainArraySingleEntry() throws Exception {
		Path testFile = tempDir.newFile("test-array-single-entry.json").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-array-single-entry.json"),
				testFile, StandardCopyOption.REPLACE_EXISTING);
		Path testFields = tempDir.newFile("test-array-json.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-array-json.csv"),
				testFields, StandardCopyOption.REPLACE_EXISTING);

		JSONSummariser.main("--input", testFile.toAbsolutePath().toString(), "--fields",
				testFields.toAbsolutePath().toString(), "--base-path", "/base");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.sum.JSONSummariser#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainArrayMultipleEntries() throws Exception {
		Path testFile = tempDir.newFile("test-array-multiple-entries.json").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-array-multiple-entries.json"),
				testFile, StandardCopyOption.REPLACE_EXISTING);
		Path testFields = tempDir.newFile("test-array-json.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-array-json.csv"),
				testFields, StandardCopyOption.REPLACE_EXISTING);

		JSONSummariser.main("--input", testFile.toAbsolutePath().toString(), "--fields",
				testFields.toAbsolutePath().toString(), "--base-path", "/base");
	}

}
