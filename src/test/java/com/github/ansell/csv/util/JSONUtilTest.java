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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonPointer;

/**
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class JSONUtilTest {

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
	}

}
