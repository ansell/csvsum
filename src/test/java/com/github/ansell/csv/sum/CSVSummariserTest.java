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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.commons.io.output.NullWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import joptsimple.OptionException;

/**
 * Tests for {@link CSVSummariser}.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVSummariserTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainNoArgs() throws Exception {
        thrown.expect(OptionException.class);
        CSVSummariser.main();
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainHelp() throws Exception {
        CSVSummariser.main("--help");
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainFileDoesNotExist() throws Exception {
        final Path testDirectory = tempDir.newFolder("test-does-not-exist").toPath();

        thrown.expect(FileNotFoundException.class);
        CSVSummariser.main("--input", testDirectory.resolve("test-does-not-exist.csv").toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainMappingFileExists() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header.csv").toPath();
        Files.copy(
                this.getClass()
                        .getResourceAsStream("/com/github/ansell/csvsum/test-single-header.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        thrown.expect(FileNotFoundException.class);
        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--output-mapping",
                testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainEmpty() throws Exception {
        final Path testFile = tempDir.newFile("test-empty.csv").toPath();
        Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvsum/test-empty.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("CSV file did not contain a valid header line");
        CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderNoLines() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header.csv").toPath();
        Files.copy(
                this.getClass()
                        .getResourceAsStream("/com/github/ansell/csvsum/test-single-header.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderOneLine() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-one-line.csv").toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-one-line.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderOneLineEscapedValue() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-one-line-escaped-value.csv")
                .toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-one-line-escaped-value.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--escape-char", "\\");
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderTroublesomeEscapes() throws Exception {
        final Path testFile = tempDir.newFile("test-escape-quotes.csv").toPath();
        Files.copy(
                this.getClass()
                        .getResourceAsStream("/com/github/ansell/csvsum/test-escape-quotes.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--escape-char", "\\",
                "--debug", "true");
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderTroublesomeEscapesWithQuoteChar() throws Exception {
        final Path testFile = tempDir.newFile("test-escape-quotes.csv").toPath();
        Files.copy(
                this.getClass()
                        .getResourceAsStream("/com/github/ansell/csvsum/test-escape-quotes.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--escape-char", "\\",
                "--debug", "true", "--quote-char", "\"");
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderOneLineFileOutput() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-one-line.csv").toPath();
        final Path testOutput = tempDir.newFile("test-single-header-one-line-output.csv").toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-one-line.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--output",
                testOutput.toAbsolutePath().toString());

        assertEquals("Did not find enough lines in the output", 2,
                Files.readAllLines(testOutput).size());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderOneLineFileOutputDebug() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-one-line.csv").toPath();
        final Path testOutput = tempDir.newFile("test-single-header-one-line-output.csv").toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-one-line.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--output",
                testOutput.toAbsolutePath().toString(), "--debug", "true");

        assertEquals("Did not find enough lines in the output", 2,
                Files.readAllLines(testOutput).size());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderOneLineFileOutputFewSamples() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-one-line.csv").toPath();
        final Path testOutput = tempDir.newFile("test-single-header-one-line-output.csv").toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-one-line.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(), "--output",
                testOutput.toAbsolutePath().toString(), "--samples", "2");

        assertEquals("Did not find enough lines in the output", 2,
                Files.readAllLines(testOutput).size());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderOneLineEmptyValue() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-one-line-empty-value.csv")
                .toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-one-line-empty-value.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderMultipleLinesWithEmptyValues() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-multiple-lines-empty-value.csv")
                .toPath();
        Files.copy(this.getClass().getResourceAsStream(
                "/com/github/ansell/csvsum/test-single-header-multiple-lines-empty-value.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderTwentyOneLines() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-twenty-one-lines.csv").toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-twenty-one-lines.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString());
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testMainSingleHeaderTwentyOneLinesOverride() throws Exception {
        final Path testFile = tempDir.newFile("test-single-header-twenty-one-lines.csv").toPath();
        Files.copy(
                this.getClass().getResourceAsStream(
                        "/com/github/ansell/csvsum/test-single-header-twenty-one-lines.csv"),
                testFile, StandardCopyOption.REPLACE_EXISTING);

        final Path testOverrideHeader = tempDir.newFile("test-header-override.csv").toPath();
        Files.write(testOverrideHeader, Arrays.asList("OverriddenHeader"), StandardCharsets.UTF_8);

        CSVSummariser.main("--input", testFile.toAbsolutePath().toString(),
                "--override-headers-file", testOverrideHeader.toAbsolutePath().toString(),
                "--header-line-count", "1");
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseInteger() throws Exception {
        final StringWriter output = new StringWriter();
        CSVSummariser.runSummarise(new StringReader("Test\n1"), output, NullWriter.NULL_WRITER,
                CSVSummariser.DEFAULT_SAMPLE_COUNT, false, false);
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseDouble() throws Exception {
        final StringWriter output = new StringWriter();
        CSVSummariser.runSummarise(new StringReader("Test\n1.0"), output, NullWriter.NULL_WRITER,
                CSVSummariser.DEFAULT_SAMPLE_COUNT, false, false);
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseAllSampleValues() throws Exception {
        final StringWriter output = new StringWriter();
        CSVSummariser.runSummarise(new StringReader("Test\n1.0"), output, NullWriter.NULL_WRITER,
                CSVSummariser.DEFAULT_SAMPLE_COUNT, false, false);
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseAllSampleValuesDebug() throws Exception {
        final StringWriter output = new StringWriter();
        CSVSummariser.runSummarise(new StringReader("Test\n1.0"), output, NullWriter.NULL_WRITER,
                -1, false, true);
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseAllSampleValuesLong() throws Exception {
        final StringWriter output = new StringWriter();
        final StringBuilder input = new StringBuilder("Test\n");
        IntStream.range(0, 1000).forEach(i -> input.append("N"));
        CSVSummariser.runSummarise(new StringReader(input.toString()), output,
                NullWriter.NULL_WRITER, -1, false, false);
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseOverrideHeaders() throws Exception {
        final StringWriter output = new StringWriter();
        final StringBuilder input = new StringBuilder("TestShouldNotBeSeen\nValue1");
        CSVSummariser.runSummarise(new StringReader(input.toString()), output,
                NullWriter.NULL_WRITER, -1, false, false, Arrays.asList("TestHeader"), 1);
        assertTrue(output.toString().contains("TestHeader"));
        assertTrue(output.toString().contains("Value1"));
        assertFalse(output.toString().contains("TestShouldNotBeSeen"));
    }

    /**
     * Test method for
     * {@link com.github.ansell.csv.sum.CSVSummariser#main(java.lang.String[])}.
     */
    @Test
    public final void testSummariseNoSampleValuesLong() throws Exception {
        final StringWriter output = new StringWriter();
        final StringBuilder input = new StringBuilder("Test\n");
        IntStream.range(0, 1000).forEach(i -> input.append("N"));
        CSVSummariser.runSummarise(new StringReader(input.toString()), output,
                NullWriter.NULL_WRITER, 0, false, true);
        System.out.println(output.toString());
    }

}
