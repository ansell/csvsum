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
package com.github.ansell.csv.util;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.ansell.csv.stream.CSVStream;
import com.github.ansell.csv.util.CSVUtil;

/**
 * Tests for {@link CSVUtil}.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class CSVUtilTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVIllegalHeader()
		throws Exception
	{
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Could not verify headers for csv file");
		CSVStream.parse(new StringReader("Header1"), h -> {
			throw new IllegalArgumentException("Did not find header: Header2");
		}, (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVEmpty()
		throws Exception
	{
		List<String> headers = new ArrayList<>();

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("CSV file did not contain a valid header line");
		CSVStream.parse(new StringReader(""), h -> headers.addAll(h), (h, l) -> l, l -> {
		});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleColumnMoreOnRow()
		throws Exception
	{
		List<String> headers = new ArrayList<>();

		thrown.expect(RuntimeException.class);
		CSVStream.parse(new StringReader("Test1\nAnswer1,Answer2,Answer3"), h -> headers.addAll(h),
				(h, l) -> l, l -> {
				});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleColumnsLessOnRow()
		throws Exception
	{
		List<String> headers = new ArrayList<>();

		thrown.expect(RuntimeException.class);
		CSVStream.parse(new StringReader("Test1, Another2, Else3\nAnswer1"), h -> headers.addAll(h),
				(h, l) -> l, l -> {
				});
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVHeaderOnlySingleColumn()
		throws Exception
	{
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1"), h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(0, lines.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVHeaderOnlyMultipleColumns()
		throws Exception
	{
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1, Another2, Else3"), h -> headers.addAll(h), (h, l) -> l,
				l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(0, lines.size());
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleRowSingleColumn()
		throws Exception
	{
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1\nAnswer1"), h -> headers.addAll(h), (h, l) -> l,
				l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(1, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVSingleRowMultipleColumns()
		throws Exception
	{
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1, Another2, Else3\nAnswer1, Alternative2, Attempt3"),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(1, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1", "Alternative2", "Attempt3")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleRowsSingleColumn()
		throws Exception
	{
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(new StringReader("Test1\nAnswer1\nAnswer2\nAnswer3"), h -> headers.addAll(h),
				(h, l) -> l, l -> lines.add(l));
		assertEquals(1, headers.size());
		assertTrue(headers.contains("Test1"));
		assertEquals(3, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1")));
		assertTrue(lines.contains(Arrays.asList("Answer2")));
		assertTrue(lines.contains(Arrays.asList("Answer3")));
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.CSVStream#parse(java.io.Reader, java.util.function.Consumer, java.util.function.BiFunction, java.util.function.Consumer)}
	 * .
	 */
	@Test
	public final void testStreamCSVMultipleRowsMultipleColumns()
		throws Exception
	{
		List<String> headers = new ArrayList<>();
		List<List<String>> lines = new ArrayList<>();

		CSVStream.parse(
				new StringReader(
						"Test1, Another2, Else3\nAnswer1, Alternative2, Attempt3\nAnswer4, Alternative5, Attempt6\nAnswer7, Alternative8, Attempt9"),
				h -> headers.addAll(h), (h, l) -> l, l -> lines.add(l));
		assertEquals(3, headers.size());
		assertTrue(headers.contains("Test1"));
		assertTrue(headers.contains("Another2"));
		assertTrue(headers.contains("Else3"));
		assertEquals(3, lines.size());
		assertTrue(lines.contains(Arrays.asList("Answer1", "Alternative2", "Attempt3")));
		assertTrue(lines.contains(Arrays.asList("Answer4", "Alternative5", "Attempt6")));
		assertTrue(lines.contains(Arrays.asList("Answer7", "Alternative8", "Attempt9")));
	}

	@Test
	public final void testWriteFullCode()
		throws Exception
	{
		List<String> headers = Arrays.asList("TestHeader1", "TestHeader2");
		List<List<String>> dataSource = Arrays.asList();
		// Or alternatively,
		// List<List<String>> dataSource = Arrays.asList(Arrays.asList("TestValue1", "TestValue2"));
		java.io.Writer writer = new StringWriter();
		CsvSchema.Builder builder = CsvSchema.builder();
		for (String nextHeader : headers) {
			builder = builder.addColumn(nextHeader);
		}
		CsvSchema schema = builder.setUseHeader(true).build();
		try (SequenceWriter csvWriter = new CsvMapper().writerWithDefaultPrettyPrinter().with(schema).forType(
				List.class).writeValues(writer);)
		{
			for (List<String> nextRow : dataSource) {
				csvWriter.write(nextRow);
			}
			// Check to see whether dataSource is empty 
			// and if so write a single empty list to trigger header output
			if (dataSource.isEmpty()) {
				csvWriter.write(Arrays.asList());
			}
		}
		System.out.println(writer.toString());
	}

	@Test
	public final void testWriteEmptySingle()
		throws Exception
	{
		List<String> headers = Arrays.asList("TestHeader1");
		StringWriter writer = new StringWriter();
		final Writer writer1 = writer;
		CSVStream.newCSVWriter(writer1, headers).write(Arrays.asList());
		System.out.println(writer.toString());
		assertEquals("TestHeader1\n", writer.toString());
	}

	@Test
	public final void testWriteEmptyAll()
		throws Exception
	{
		List<String> headers = Arrays.asList("TestHeader1");
		StringWriter writer = new StringWriter();
		final Writer writer1 = writer;
		CSVStream.newCSVWriter(writer1, headers).writeAll(Arrays.asList(Arrays.asList()));
		System.out.println(writer.toString());
		assertEquals("TestHeader1\n", writer.toString());
	}

	@Test
	public final void testWriteSingleEmptyString()
		throws Exception
	{
		List<String> headers = Arrays.asList("TestHeader1");
		StringWriter writer = new StringWriter();
		final Writer writer1 = writer;
		CSVStream.newCSVWriter(writer1, headers).writeAll(Arrays.asList(Arrays.asList("Z")));
		System.out.println(writer.toString());
		assertEquals("TestHeader1\nZ\n", writer.toString());

		AtomicBoolean headersGood = new AtomicBoolean(false);
		AtomicBoolean lineGood = new AtomicBoolean(false);
		CSVStream.parse(new StringReader(writer.toString()), h -> {
			if (headers.size() == 1 && headers.contains("TestHeader1")) {
				headersGood.set(true);
			}
		}, (h, l) -> {
			if (l.size() == 1 && l.get(0).equals("Z")) {
				lineGood.set(true);
			}
			return l;
		}, l -> {
		});

		assertTrue("Headers were not recognised", headersGood.get());
		assertTrue("Line was not recognised", lineGood.get());
	}
	
	@Test
	public final void testParseInts() throws Exception
	{
        final String input =
        		"x,y\n"
        		+"1,1\n"
        		+"2,8\n"
        		+"3,2\n"
        		+"4,4\n"
        		+"5,5\n"
        		+"6,0\n"
        		+"7,10\n"
        		+"8,-4\n"
                ;
        
        
        CSVStream.parse(new StringReader(input), h -> {}, (h, l) -> l, l -> {});
	}
}
