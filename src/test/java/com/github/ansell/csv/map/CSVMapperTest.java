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

import com.github.ansell.csv.map.CSVMapper;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.stream.CSVStreamException;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVMapper}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVMapperTest {

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
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainNoArgs() throws Exception {
		thrown.expect(OptionException.class);
		CSVMapper.main();
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainHelp() throws Exception {
		CSVMapper.main("--help");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainFileDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMapper.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainMappingDoesNotExist() throws Exception {
		Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

		thrown.expect(FileNotFoundException.class);
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testDirectory.resolve("test-does-not-exist.csv").toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainEmpty() throws Exception {
		Path testFile = tempDir.newFile("test-empty.csv").toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"), testFile,
				StandardCopyOption.REPLACE_EXISTING);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainComplete() throws Exception {
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFile() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString(), "--output",
				testDirectory.resolve("test-output.csv").toString());

		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(testDirectory.resolve("test-output.csv"));) {
			CSVStream.parse(reader, h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		}
		assertEquals(8, headers.size());
		assertEquals(4, lines.size());
		lines.sort(Comparator.comparing(l -> l.get(0)));

		lines.get(0).forEach(k -> System.out.print("\"" + k + "\", "));

		assertEquals(Arrays.asList("anotherfield", "Field2", "varietyOrSubspecies", "Field3", "naturalised", "Field4",
				"unrelatedField", "Field5"), headers);
		assertEquals(Arrays.asList("A1", "A2", "", "A3", "", "A4", "Useful", "A5a"), lines.get(0));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFileAppend() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		Path existingOutput = testDirectory.resolve("test-output.csv");
		try (Writer existingOutputWriter = Files.newBufferedWriter(existingOutput, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE_NEW);) {
			existingOutputWriter
					.write("anotherfield,Field2,varietyOrSubspecies,Field3,naturalised,Field4,unrelatedField,Field5\n");
			existingOutputWriter.write("Z1,Z2,Z2A,Z3,Z3A,Z4,Z4A,Z5\n");
		}

		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString(), "--output", existingOutput.toString(), "--append-to-existing",
				"true");

		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(existingOutput);) {
			CSVStream.parse(reader, h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		}
		assertEquals(8, headers.size());
		assertEquals(5, lines.size());
		lines.sort(Comparator.comparing(l -> l.get(0)));

		// lines.get(0).forEach(k -> System.out.print("\"" + k + "\", "));

		assertEquals(Arrays.asList("anotherfield", "Field2", "varietyOrSubspecies", "Field3", "naturalised", "Field4",
				"unrelatedField", "Field5"), headers);
		assertEquals(Arrays.asList("A1", "A2", "", "A3", "", "A4", "Useful", "A5a"), lines.get(0));

		Files.copy(existingOutput, System.out);
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFileAppendHeaderListDifferentElements() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		Path existingOutput = testDirectory.resolve("test-output.csv");
		try (Writer existingOutputWriter = Files.newBufferedWriter(existingOutput, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE_NEW);) {
			// Create previous with "Field6" at end instead of "Field5" to check that it fails
			existingOutputWriter
					.write("anotherfield,Field2,varietyOrSubspecies,Field3,naturalised,Field4,unrelatedField,Field6\n");
			existingOutputWriter.write("Z1,Z2,Z2A,Z3,Z3A,Z4,Z4A,Z5\n");
		}

		thrown.expect(CSVStreamException.class);
		thrown.expectMessage("Could not verify headers for csv file");
		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMapping.toAbsolutePath().toString(), "--output", existingOutput.toString(), "--append-to-existing",
				"true");
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFileHidden() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMappingHidden.toAbsolutePath().toString(), "--output",
				testDirectory.resolve("test-output.csv").toString());

		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(testDirectory.resolve("test-output.csv"));) {
			CSVStream.parse(reader, h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		}
		assertEquals(7, headers.size());
		assertEquals(6, lines.size());
		lines.sort(Comparator.comparing(l -> l.get(0)));

		lines.get(0).forEach(k -> System.out.print("\"" + k + "\", "));

		assertEquals(Arrays.asList("A1", "A2", "", "A3", "", "A4", "Useful"), lines.get(0));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.map.CSVMapper#main(java.lang.String[])}.
	 */
	@Test
	public final void testMainCompleteWithOutputFilePrevious() throws Exception {
		Path testDirectory = tempDir.newFolder("test").toPath();

		CSVMapper.main("--input", testFile.toAbsolutePath().toString(), "--mapping",
				testMappingPrevious.toAbsolutePath().toString(), "--output",
				testDirectory.resolve("test-output.csv").toString());

		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(testDirectory.resolve("test-output.csv"));) {
			CSVStream.parse(reader, h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		}
		assertEquals(4, headers.size());
		assertEquals(6, lines.size());
		lines.sort(Comparator.comparing(l -> l.get(0)));

		lines.get(0).forEach(k -> System.out.print("\"" + k + "\", "));

		assertEquals(Arrays.asList("dummy-value", "A1", "no-previous", "no-map-previous"), lines.get(0));
		assertEquals(Arrays.asList("dummy-value", "B1", "found-previousA1", "found-map-previousno-previous"),
				lines.get(1));
		assertEquals(Arrays.asList("dummy-value", "C1", "found-previousB1", "found-map-previousfound-previousA1"),
				lines.get(2));
		assertEquals(Arrays.asList("dummy-value", "D1", "found-previousC1", "found-map-previousfound-previousB1"),
				lines.get(3));
		assertEquals(Arrays.asList("dummy-value", "E1", "found-previousD1", "found-map-previousfound-previousC1"),
				lines.get(4));
		assertEquals(Arrays.asList("dummy-value", "F1", "found-previousE1", "found-map-previousfound-previousD1"),
				lines.get(5));
	}
}
