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
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.fasterxml.jackson.databind.JsonNode;
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

	@Ignore("Requires offline resource")
	@Test
	public final void testToPrettyPrintLarge() throws Exception {
		try (Writer output = Files.newBufferedWriter(Paths.get("/tmp/test-pretty.json"), StandardCharsets.UTF_8);
				Reader input = Files.newBufferedReader(Paths.get("/tmp/test.json"), StandardCharsets.UTF_8);) {
			JSONUtil.toPrettyPrint(input, output);
		}
	}

	// @Ignore("ALA website is broken w.r.t chunked encoding and GitHub is picky
	// about lots of things")
	@Test
	public final void testPrettyPrintURL() throws Exception {
		StringWriter output = new StringWriter();
		try (InputStream inputStream = JSONUtil
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
	public final void testPrettyPrintNode() throws Exception {
		StringReader input = new StringReader("{\"owner\": {\"avatar_url\": \"https://avatar.githubusercontent.com/\"}}");
		StringWriter output = new StringWriter();
		JsonNode jsonNode = JSONUtil.loadJSON(input);
		JSONUtil.toPrettyPrint(jsonNode, output);

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
	 * Test method for
	 * {@link JSONUtil#queryJSONPost(String, java.util.Map, String)}.
	 */
	@Ignore("images.ala.org.au stopped returning a result in this case, so test broken")
	@Test
	public final void testQueryJSONPost() throws Exception {
		Map<String, Object> postVariables = new LinkedHashMap<>();
		// postVariables.put("filenames", Arrays.asList("X01860.mp3"));
		// postVariables.put("filenames", "X01860.mp3");
		// postVariables.put("filenames", Arrays.asList("X01860"));
		// postVariables.put("filenames",
		// Arrays.asList("file:///data/biocache-media/dr341/13723/c5d7dd2a-1b3f-4e7c-a5ab-02136008a4e9/X01860.mp3"));
		// postVariables.put("filenames", Arrays.asList("dr341"));
		// String result =
		// JSONUtil.queryJSONPost("http://images.ala.org.au/ws/findImagesByOriginalFilename",
		// postVariables , "/");
		// postVariables.put("q", "X01860.mp3");
		// postVariables.put("key", "originalFilename");
		// String result =
		// JSONUtil.queryJSONPost("http://images.ala.org.au/ws/getImageLinksForMetaDataValues",
		// postVariables , "/");
		postVariables.put("key", "originalFilename");
		postVariables.put("values", Arrays.asList("X01860.mp3"));

		String completeResult = JSONUtil.queryJSONPost("http://images.ala.org.au/ws/findImagesByMetadata",
				postVariables, "/");

		System.out.println("Result was" + completeResult);

		String result = JSONUtil.queryJSONPost("" + "" + "" + "", postVariables, "/images/X01860.mp3/0/imageUrl");

		assertEquals("http://images.ala.org.au/store/5/c/9/4/be55c430-29c4-4991-a160-0bbb4bdc49c5/original", result);
	}

	/**
	 * Test method for {@link JSONUtil#queryJSON(String, String)}.
	 */
	@Ignore
	@Test
	public final void testQueryJSONImageSearch() throws Exception {
		Map<String, Object> postVariables = new LinkedHashMap<>();
		String result = JSONUtil.queryJSONPost(
				"http://images.ala.org.au/ws/getImageLinksForMetaDataValues?key=originalFilename&q=X01860.mp3",
				postVariables, "/");
		// String result =
		// JSONUtil.queryJSON("http://images.ala.org.au/ws/getImageLinksForMetaDataValues?key=originalFilename&q=X01860.mp3",
		// "/");
		System.out.println(result);
	}

	@Test
	public final void testLoadJSONNullProperty() throws Exception {
		Path testNullFile = tempDir.newFolder("jsonutiltest").toPath().resolve("test-null.json");
		Files.copy(this.getClass().getResourceAsStream("/com/github/ansell/csvmap/test-null.json"), testNullFile);
		JsonNode rootNode = JSONUtil.loadJSON(testNullFile);

		JsonNode searchResultsNode = rootNode.get("searchResults");

		JsonNode pageSizeNode = searchResultsNode.get("pageSize");

		JsonNode startIndexNode = searchResultsNode.get("startIndex");

		System.out.println("pageSize=" + pageSizeNode.toString());
		System.out.println("startIndex=" + startIndexNode.toString());
	}
}
