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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.JsonPointer;
import com.github.jsonldjava.utils.JsonUtils;

/**
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONUtilTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.JSONUtil#toPrettyPrint(java.io.Reader, java.io.Writer)}
	 * .
	 */
	@Test
	public final void testToPrettyPrint() throws Exception {
		StringWriter output = new StringWriter();
		StringReader input = new StringReader("{\"test\":\"something\"}");
		JSONUtil.toPrettyPrint(input, output);
		System.out.println(output.toString());
		assertTrue(output.toString().contains("\"test\" : \"something\""));
	}

	//@Ignore("ALA website is broken w.r.t chunked encoding and GitHub is picky about lots of things")
	@Test
	public final void testPrettyPrintURL() throws Exception {
		StringWriter output = new StringWriter();
		try (InputStream inputStream = JsonUtils
				.openStreamForURL(new java.net.URL("https://api.github.com/repos/ansell/csvsum"
		// "http://bie.ala.org.au/search.json?q=Banksia+occidentalis"
		), JsonUtils.getDefaultHttpClient()); Reader input = new BufferedReader(new InputStreamReader(inputStream));) {
			JSONUtil.toPrettyPrint(input, output);
		}

		System.out.println(output.toString());

		String avatar = JSONUtil.queryJSON(new StringReader(output.toString()), "/owner/avatar_url");

		assertTrue(avatar, avatar.contains(".githubusercontent.com/"));
		assertTrue(avatar, avatar.startsWith("https://avatar"));

		// JSONUtil.queryJSON(
		// "http://bie.ala.org.au/search.json?q=Banksia+occidentalis",
		// "/searchResults/results/0/guid");
	}

	@Test
	public final void testQuery() throws Exception {
		Path testFile = tempDir.newFile().toPath();
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/ala-test.json"), testFile,
				StandardCopyOption.REPLACE_EXISTING);
		try (Reader reader = Files.newBufferedReader(testFile);) {
			JSONUtil.toPrettyPrint(reader, new OutputStreamWriter(System.out));
		}
		System.out.println("");
		try (Reader reader = Files.newBufferedReader(testFile);) {
			System.out.println(JSONUtil.queryJSON(reader, JsonPointer.compile("/searchResults/results/0/guid")));
		}
	}

	/**
	 * Test method for
	 * {@link com.github.ansell.csv.util.JSONUtil#queryJSON(java.io.Reader, JsonPointer)}
	 * .
	 */
	@Test
	public final void testQueryJSON() throws Exception {
		StringReader input = new StringReader("{ \"test\": \"something\"}");
		JsonPointer jpath = JsonPointer.compile("/test");
		String result = JSONUtil.queryJSON(input, jpath);
		System.out.println(result);
		assertEquals("something", result);

		StringReader input2 = new StringReader("{ \"test\": \"something\"}");
		String result2 = JSONUtil.queryJSON(input2, "/test");
		System.out.println(result2);
		assertEquals("something", result2);
	}
	
	/**
	 * Test method for {@link JSONUtil#queryJSONPost(String, java.util.Map, String)}.
	 */
	@Test
	public final void testQueryJSONPost() throws Exception {
		Map<String, Object> postVariables = new LinkedHashMap<>();
		postVariables.put("filenames", Arrays.asList("X01860.mp3"));
		//postVariables.put("filenames", "X01860.mp3");
		String result = JSONUtil.queryJSONPost("http://images.ala.org.au/ws/findImagesByOriginalFilename", postVariables , "/");
		
		//assertEquals(result, "TODO: What should I be");
	}
}
