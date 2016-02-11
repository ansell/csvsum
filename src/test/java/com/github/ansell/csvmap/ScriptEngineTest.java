package com.github.ansell.csvmap;

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
