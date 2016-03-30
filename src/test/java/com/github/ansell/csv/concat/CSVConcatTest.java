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
package com.github.ansell.csv.concat;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.github.ansell.csv.concat.CSVConcat;
import com.github.ansell.csv.join.CSVJoiner;
import com.github.ansell.csv.util.CSVUtil;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVConcat}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVConcatTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private Path testMapping;

	private Path testMappingHidden;

	private Path testFile;

	private Path testOtherFile;

	private Path testMappingMulti;

	private Path testFileMulti;

	private Path testOtherFileMulti;

	private Path testMappingMultiDots;

	private Path testFileMultiDots;

	private Path testOtherFileMultiDots;

	@Before
	public void setUp() throws Exception {
		testMapping = tempDir.newFile("test-mapping.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-mapping.csv"), testMapping,
				StandardCopyOption.REPLACE_EXISTING);
		testMappingHidden = tempDir.newFile("test-mapping-with-hidden.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-mapping-with-hidden.csv"),
				testMappingHidden, StandardCopyOption.REPLACE_EXISTING);
		testFile = tempDir.newFile("test-source.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
		testOtherFile = tempDir.newFile("test-source-other.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source-other.csv"),
				testOtherFile, StandardCopyOption.REPLACE_EXISTING);

		testMappingMulti = tempDir.newFile("test-mapping-multi-key.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-mapping-multi-key.csv"),
				testMappingMulti, StandardCopyOption.REPLACE_EXISTING);
		testFileMulti = tempDir.newFile("test-source-multi-key.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source-multi-key.csv"),
				testFileMulti, StandardCopyOption.REPLACE_EXISTING);
		testOtherFileMulti = tempDir.newFile("test-source-other-multi-key.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source-other-multi-key.csv"),
				testOtherFileMulti, StandardCopyOption.REPLACE_EXISTING);

		testMappingMultiDots = tempDir.newFile("test-mapping-multi-key-dots.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-mapping-multi-key-dots.csv"),
				testMappingMultiDots, StandardCopyOption.REPLACE_EXISTING);
		testFileMultiDots = tempDir.newFile("test-source-multi-key-dots.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source-multi-key-dots.csv"),
				testFileMultiDots, StandardCopyOption.REPLACE_EXISTING);
		testOtherFileMultiDots = tempDir.newFile("test-source-other-multi-key-dots.csv").toPath();
		Files.copy(
				this.getClass().getResourceAsStream("/com/github/ansell/csvjoin/test-source-other-multi-key-dots.csv"),
				testOtherFileMultiDots, StandardCopyOption.REPLACE_EXISTING);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVConcat.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		CSVConcat.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVConcat.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--other-input",
				testOtherFile.toAbsolutePath().toString(), "--mapping", testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainOtherFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVConcat.main("--input", testFile.toAbsolutePath().toString(), "--other-input",
				testDirectory.resolve("test-does-not-exist.csv").toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMappingDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVConcat.main("--input", testFile.toAbsolutePath().toString(), "--other-input",
				testOtherFile.toAbsolutePath().toString(), "--mapping",
				testDirectory.resolve("test-does-not-exist.csv").toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVConcat.main("--input", testFile.toAbsolutePath().toString(), "--other-input",
				testOtherFile.toAbsolutePath().toString(), "--mapping", testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSimple() throws Exception {
		Path testInputSimple = tempDir.newFile("test-input-simple.csv").toPath();
		Files.write(testInputSimple, Arrays.asList("primaryKeyField,input1Field", "1,value1"));
		Path testInputSimpleOther = tempDir.newFile("test-input-simple-other.csv").toPath();
		Files.write(testInputSimpleOther, Arrays.asList("primaryKeyField,input2Field", "1,value2"));
		Path testMappingSimple = tempDir.newFile("test-mapping-simple.csv").toPath();
		Files.write(testMappingSimple,
				Arrays.asList("OldField,NewField,Shown,Language,Mapping", "primaryKeyField,primaryKeyField,,CsvJoin,primaryKeyField",
						"input1Field,input1Field,,,", "input2Field,input2Field,,,"));
		Path testOutput = tempDir.newFile("test-output-simple.csv").toPath();
		CSVConcat.main("--input", testInputSimple.toAbsolutePath().toString(), "--other-input",
				testInputSimpleOther.toAbsolutePath().toString(), "--mapping",
				testMappingSimple.toAbsolutePath().toString(), "--output",
				testOutput.toAbsolutePath().toString());
		Files.copy(testOutput, System.out);
		List<String> testAllLines = Files.readAllLines(testOutput);
		assertEquals(2, testAllLines.size());
		assertEquals("primaryKeyField,input1Field,input2Field", testAllLines.get(0));
		assertEquals("1,value1,value2", testAllLines.get(1));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVConcat#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainSimpleCsvJoin() throws Exception {
		Path testInputSimple = tempDir.newFile("test-input-simple.csv").toPath();
		Files.write(testInputSimple, Arrays.asList("primaryKeyField,input1Field", "1,value1"));
		Path testInputSimpleOther = tempDir.newFile("test-input-simple-other.csv").toPath();
		Files.write(testInputSimpleOther, Arrays.asList("primaryKeyField,input2Field", "1,value2"));
		Path testMappingSimple = tempDir.newFile("test-mapping-simple.csv").toPath();
		Files.write(testMappingSimple,
				Arrays.asList("OldField,NewField,Shown,Language,Mapping", "primaryKeyField,primaryKeyField,,CsvJoin,primaryKeyField",
						"input1Field,input1Field,,,", "input2Field,input2Field,,,"));
		Path testOutput = tempDir.newFile("test-output-simple.csv").toPath();
		CSVJoiner.main("--input", testInputSimple.toAbsolutePath().toString(), "--other-input",
				testInputSimpleOther.toAbsolutePath().toString(), "--mapping",
				testMappingSimple.toAbsolutePath().toString(), "--output",
				testOutput.toAbsolutePath().toString());
		Files.copy(testOutput, System.out);
		List<String> testAllLines = Files.readAllLines(testOutput);
		assertEquals(2, testAllLines.size());
		assertEquals("primaryKeyField,input1Field,input2Field", testAllLines.get(0));
		assertEquals("1,value1,value2", testAllLines.get(1));
	}

}
