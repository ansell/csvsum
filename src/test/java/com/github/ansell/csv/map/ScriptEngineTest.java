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

import static org.junit.Assert.*;

import java.util.List;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ScriptEngineTest {

	private static final ScriptEngineManager SCRIPT_MANAGER = new ScriptEngineManager();

	// Uncomment the following to debug which script engines are available on
	// the classpath
	static {
		List<ScriptEngineFactory> factories = SCRIPT_MANAGER.getEngineFactories();

		System.out.println("Installed script engines:");

		for (ScriptEngineFactory nextFactory : factories) {
			System.out.println(nextFactory.getEngineName());
		}
	}

	private ScriptEngine scriptEngine;

	@Before
	public void setUp() throws Exception {
		scriptEngine = SCRIPT_MANAGER.getEngineByName("lua");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void test() throws Exception {
		String csvMapperScript = "mapFunction = function(inputHeaders, inputField, inputValue, outputField, line) return inputValue end";
		String simpleScript = "return inputValue";
		CompiledScript compiledScript = (CompiledScript) ((Compilable) scriptEngine).compile(simpleScript);
		Bindings bindings = scriptEngine.createBindings();
		// inputHeaders, inputField, inputValue, outputField, line
		bindings.put("inputHeaders", "");
		bindings.put("inputField", "");
		bindings.put("inputValue", "testreturnvalue");
		bindings.put("outputField", "");
		bindings.put("line", "");
		String result = (String) compiledScript.eval(bindings);
		System.out.println(result);
		assertEquals("testreturnvalue", result);
	}

}
