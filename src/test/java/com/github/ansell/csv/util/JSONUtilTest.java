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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ansell.csv.stream.util.JSONStreamUtil;
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

    @Ignore("Ignore so AEKOS API outages don't affect test results")
    @Test
    public final void testAEKOS() throws Exception {
        final StringWriter output = new StringWriter();
        final JsonNode jsonNode = JSONUtil.httpGetJSON(
                "https://dev.api.aekos.org.au/v2/allSpeciesData.json?start=0&rows=5", 10, 10,
                TimeUnit.MILLISECONDS);
        JSONStreamUtil.toPrettyPrint(jsonNode, output);
        System.out.println(output.toString());
    }

    @Ignore("Requires offline resource")
    @Test
    public final void testToPrettyPrintLarge() throws Exception {
        try (Writer output = Files.newBufferedWriter(Paths.get("/tmp/test-pretty.json"),
                StandardCharsets.UTF_8);
                Reader input = Files.newBufferedReader(Paths.get("/tmp/test.json"),
                        StandardCharsets.UTF_8);) {
            JSONStreamUtil.toPrettyPrint(input, output);
        }
    }

    @Ignore("Github is frequently unavailable from TravisCI")
    @Test
    public final void testPrettyPrintURL() throws Exception {
        final StringWriter output = new StringWriter();
        try (InputStream inputStream = JSONUtil
                .openStreamForURL(new java.net.URL("https://api.github.com/repos/ansell/csvsum"
                // "http://bie.ala.org.au/search.json?q=Banksia+occidentalis"
                ), JsonUtils.getDefaultHttpClient());
                Reader input = new BufferedReader(new InputStreamReader(inputStream));) {
            JSONStreamUtil.toPrettyPrint(input, output);
        }

        System.out.println(output.toString());

        final String avatar = JSONStreamUtil.queryJSON(new StringReader(output.toString()),
                "/owner/avatar_url");

        assertTrue(avatar, avatar.contains(".githubusercontent.com/"));
        assertTrue(avatar, avatar.startsWith("https://avatar"));

        // JSONUtil.queryJSON(
        // "http://bie.ala.org.au/search.json?q=Banksia+occidentalis",
        // "/searchResults/results/0/guid");
    }

    @Ignore("Github is frequently unavailable from TravisCI")
    @Test
    public final void testHttpGetJSON() throws Exception {
        final StringWriter output = new StringWriter();
        final JsonNode jsonNode = JSONUtil
                .httpGetJSON("https://api.github.com/repos/ansell/csvsum");
        JSONStreamUtil.toPrettyPrint(jsonNode, output);

        System.out.println(output.toString());

        final String avatar = JSONStreamUtil.queryJSON(new StringReader(output.toString()),
                "/owner/avatar_url");

        assertTrue(avatar, avatar.contains(".githubusercontent.com/"));
        assertTrue(avatar, avatar.startsWith("https://avatar"));

        // JSONUtil.queryJSON(
        // "http://bie.ala.org.au/search.json?q=Banksia+occidentalis",
        // "/searchResults/results/0/guid");
    }

    @Ignore("Github is frequently unavailable from TravisCI")
    @Test
    public final void testHttpGetJSONWithRetry() throws Exception {
        final StringWriter output = new StringWriter();
        final JsonNode jsonNode = JSONUtil.httpGetJSON("https://api.github.com/repos/ansell/csvsum",
                1, 1, TimeUnit.SECONDS);
        JSONStreamUtil.toPrettyPrint(jsonNode, output);

        System.out.println(output.toString());

        final String avatar = JSONStreamUtil.queryJSON(new StringReader(output.toString()),
                "/owner/avatar_url");

        assertTrue(avatar, avatar.contains(".githubusercontent.com/"));
        assertTrue(avatar, avatar.startsWith("https://avatar"));

        // JSONUtil.queryJSON(
        // "http://bie.ala.org.au/search.json?q=Banksia+occidentalis",
        // "/searchResults/results/0/guid");
    }

    /**
     * Test method for
     * {@link JSONUtil#queryJSONPost(String, java.util.Map, String)}.
     */
    @Ignore("images.ala.org.au stopped returning a result in this case, so test broken")
    @Test
    public final void testQueryJSONPost() throws Exception {
        final Map<String, Object> postVariables = new LinkedHashMap<>();
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

        final String completeResult = JSONUtil.queryJSONPost(
                "http://images.ala.org.au/ws/findImagesByMetadata", postVariables, "/");

        System.out.println("Result was" + completeResult);

        final String result = JSONUtil.queryJSONPost("" + "" + "" + "", postVariables,
                "/images/X01860.mp3/0/imageUrl");

        assertEquals(
                "http://images.ala.org.au/store/5/c/9/4/be55c430-29c4-4991-a160-0bbb4bdc49c5/original",
                result);
    }

    /**
     * Test method for
     * {@link JSONUtil#queryJSONPost(String, java.util.Map, String)}.
     */
    @Ignore
    @Test
    public final void testQueryJSONPostByDataResourceUID() throws Exception {
        final Map<String, Object> postVariables = new LinkedHashMap<>();
        postVariables.put("key", "dataResourceUid");
        postVariables.put("values", Arrays.asList("dr341"));

        final String completeResult = JSONUtil.queryJSONPost(
                "http://images.ala.org.au/ws/findImagesByMetadata", postVariables, "/");

        System.out.println("Result was" + completeResult);

        // String result = JSONUtil.queryJSONPost("" + "" + "" + "",
        // postVariables, "/images/X01860.mp3/0/imageUrl");

        // assertEquals("http://images.ala.org.au/store/5/c/9/4/be55c430-29c4-4991-a160-0bbb4bdc49c5/original",
        // result);
    }

    /**
     * Test method for
     * {@link JSONUtil#queryJSONPost(String, java.util.Map, String)}.
     */
    @Ignore("TODO: Determine which webservice to use to replicate the advanced search functionality that is HTML only")
    @Test
    public final void testHttpGetJsonCriteria() throws Exception {
        final JsonNode completeResult = JSONUtil.httpGetJSON(
                "http://images.ala.org.au/ws/search/index.json?searchCriteriaDefinitionId=85278494&fieldValue=dr341");

        System.out.println("Result was" + JSONStreamUtil.toPrettyPrint(completeResult));

        // String result = JSONUtil.queryJSONPost("" + "" + "" + "",
        // postVariables, "/images/X01860.mp3/0/imageUrl");

        // assertEquals("http://images.ala.org.au/store/5/c/9/4/be55c430-29c4-4991-a160-0bbb4bdc49c5/original",
        // result);
    }

    /**
     * Test method for {@link JSONUtil#queryJSON(String, String)}.
     */
    @Ignore
    @Test
    public final void testQueryJSONImageSearch() throws Exception {
        final Map<String, Object> postVariables = new LinkedHashMap<>();
        final String result = JSONUtil.queryJSONPost(
                "http://images.ala.org.au/ws/getImageLinksForMetaDataValues?key=originalFilename&q=X01860.mp3",
                postVariables, "/");
        // String result =
        // JSONUtil.queryJSON("http://images.ala.org.au/ws/getImageLinksForMetaDataValues?key=originalFilename&q=X01860.mp3",
        // "/");
        System.out.println(result);
    }

}
