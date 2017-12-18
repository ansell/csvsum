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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csv.map.JSONMapper;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.CSVStreamException;

import joptsimple.OptionException;

/**
 * Tests for {@link JSONMapper}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONMapperTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private Path testMapping;

	private Path testMappingHidden;

	private Path testMappingPrevious;

	private Path testFile;

	@Before
	public void setUp() throws Exception {
		testMapping = tempDir.newFile("test-mapping.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-mapping.csv"), testMapping,
				StandardCopyOption.REPLACE_EXISTING);
		testMappingHidden = tempDir.newFile("test-mapping-with-hidden.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-mapping-with-hidden.csv"),
				testMappingHidden, StandardCopyOption.REPLACE_EXISTING);
		testMappingPrevious = tempDir.newFile("test-mapping-previous.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-mapping-previous.csv"),
				testMappingPrevious, StandardCopyOption.REPLACE_EXISTING);
		testFile = tempDir.newFile("test-source.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-source.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.JSONMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		JSONMapper.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.JSONMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		JSONMapper.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.JSONMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		JSONMapper.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.JSONMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMappingDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		JSONMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testDirectory.resolve("test-does-not-exist.csv").toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.JSONMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		JSONMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.JSONMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainComplete() throws Exception {
		JSONMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

}
