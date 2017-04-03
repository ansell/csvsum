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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.http.HttpVersion;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.cache.BasicHttpCacheStorage;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.utils.JarCacheStorage;

/**
 * JSON utilities used by CSV processors.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONUtil {

    private static final String ACCEPT_HEADER = "application/json, application/javascript;q=0.5, text/javascript;q=0.5, text/plain;q=0.2, */*;q=0.1";
	private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
	private static final JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);
	private static volatile CloseableHttpClient DEFAULT_HTTP_CLIENT = null;

	public static JsonNode httpGetJSON(String url) throws JsonProcessingException, IOException {
		try (final InputStream stream = openStreamForURL(new java.net.URL(url),
				getDefaultHttpClient());
				final Reader input = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));) {
			return JSON_MAPPER.readTree(input);
		}
	}

	public static String queryJSON(String url, String jpath) throws JsonProcessingException, IOException {
		return queryJSON(url, JsonPointer.compile(jpath));
	}

	public static String queryJSON(String url, JsonPointer jpath) throws JsonProcessingException, IOException {
		try (final InputStream stream = openStreamForURL(new java.net.URL(url),
				getDefaultHttpClient());
				final Reader input = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));) {
			return queryJSON(input, jpath);
		}
	}

	public static JsonNode loadJSON(Path path) throws JsonProcessingException, IOException {
		try(final Reader input = Files.newBufferedReader(path, StandardCharsets.UTF_8);) {
			return JSON_MAPPER.readTree(input);
		}
	}
	
	public static String queryJSON(Reader input, String jpath) throws JsonProcessingException, IOException {
		return queryJSON(input, JsonPointer.compile(jpath));
	}

	public static String queryJSON(Reader input, JsonPointer jpath) throws JsonProcessingException, IOException {
		JsonNode root = JSON_MAPPER.readTree(input);
		return root.at(jpath).asText();
	}

	public static String queryJSONPost(String url, Map<String, Object> postVariables, String jpath)
			throws JsonProcessingException, IOException {

		StringWriter serialisedVariables = new StringWriter();
		
		toPrettyPrint(postVariables, serialisedVariables);
		
		String postVariableString = serialisedVariables.toString();
		
		String result = Request.Post(url).useExpectContinue().version(HttpVersion.HTTP_1_1)
				.bodyString(postVariableString, ContentType.DEFAULT_TEXT).execute().returnContent()
				.asString(StandardCharsets.UTF_8);

		String result2 = queryJSON(new StringReader(result), jpath);
		
		return result2;
	}

	public static JsonNode httpPostJSON(String url, Map<String, Object> postVariables)
			throws JsonProcessingException, IOException {

		StringWriter serialisedVariables = new StringWriter();
		
		toPrettyPrint(postVariables, serialisedVariables);
		
		String postVariableString = serialisedVariables.toString();
		
		try(InputStream inputStream = Request.Post(url).useExpectContinue().version(HttpVersion.HTTP_1_1)
				.bodyString(postVariableString, ContentType.DEFAULT_TEXT).execute().returnContent()
				.asStream();
				Reader inputReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
			return JSON_MAPPER.readTree(inputReader);
		}		
	}

	public static void toPrettyPrint(Reader input, Writer output) throws IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(output);
		jw.useDefaultPrettyPrinter();
		JsonParser parser = JSON_FACTORY.createParser(input);
		jw.writeObject(parser.readValueAsTree());
	}

	public static void toPrettyPrint(Map<String, Object> input, Writer output) throws IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(output);
		jw.useDefaultPrettyPrinter();
		jw.writeObject(input);
	}

    static InputStream openStreamForURL(java.net.URL url, CloseableHttpClient httpClient) throws IOException {
        final String protocol = url.getProtocol();
        if (!protocol.equalsIgnoreCase("http") && !protocol.equalsIgnoreCase("https")) {
            return url.openStream();
        }
        final HttpUriRequest request = new HttpGet(url.toExternalForm());
        request.addHeader("Accept", ACCEPT_HEADER);
    
        try (final CloseableHttpResponse response = httpClient.execute(request);) {
            final int status = response.getStatusLine().getStatusCode();
            if (status != 200 && status != 203) {
                throw new IOException("Can't retrieve " + url + ", status code: " + status);
            }
            return response.getEntity().getContent();
        }
    }
    
    private static CloseableHttpClient getDefaultHttpClient() {
        CloseableHttpClient result = DEFAULT_HTTP_CLIENT;
        if (result == null) {
            synchronized (JSONUtil.class) {
                result = DEFAULT_HTTP_CLIENT;
                if (result == null) {
                    result = DEFAULT_HTTP_CLIENT = JSONUtil.createDefaultHttpClient();
                }
            }
        }
        return result;
    }

    private static CloseableHttpClient createDefaultHttpClient() {
        // Common CacheConfig for both the JarCacheStorage and the underlying
        // BasicHttpCacheStorage
        final CacheConfig cacheConfig = CacheConfig.custom().setMaxCacheEntries(1000)
                .setMaxObjectSize(1024 * 128).build();
    
        CloseableHttpClient result = CachingHttpClientBuilder
                .create()
                // allow caching
                .setCacheConfig(cacheConfig)
                // Wrap the local JarCacheStorage around a BasicHttpCacheStorage
                .setHttpCacheStorage(
                        new JarCacheStorage(null, cacheConfig, new BasicHttpCacheStorage(
                                cacheConfig)))
                // Support compressed data
                // http://hc.apache.org/httpcomponents-client-ga/tutorial/html/httpagent.html#d5e1238
                .addInterceptorFirst(new RequestAcceptEncoding())
                .addInterceptorFirst(new ResponseContentEncoding())
                // use system defaults for proxy etc.
                .useSystemProperties().build();
    
        return result;
    }
}
