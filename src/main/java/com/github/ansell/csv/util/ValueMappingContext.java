/*
 * Copyright (c) 2017, Peter Ansell
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ansell.jdefaultdict.JDefaultDict;

/**
 * ss Context object sent to ValueMapping function to avoid method parameter
 * overloading.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ValueMappingContext {

    private final List<String> inputHeaders;
    private final List<String> line;
    private final List<String> previousLine;
    private final List<String> previousMappedLine;
    private final List<ValueMapping> mappings;
    private final JDefaultDict<String, Set<String>> primaryKeys;
    private final int lineNumber;
    private final int filteredLineNumber;
    private final BiConsumer<List<String>, List<String>> mapLineConsumer;
    private final List<String> outputHeaders;
    private final Map<String, String> defaultValues;
    private final Optional<JsonNode> jsonNode;
    private final JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts;

    public ValueMappingContext(List<String> inputHeaders, List<String> line,
            List<String> previousLine, List<String> previousMappedLine, List<ValueMapping> map,
            JDefaultDict<String, Set<String>> primaryKeys,
            JDefaultDict<String, JDefaultDict<String, AtomicInteger>> valueCounts, int lineNumber,
            int filteredLineNumber, BiConsumer<List<String>, List<String>> mapLineConsumer,
            List<String> outputHeaders, Map<String, String> defaultValues,
            Optional<JsonNode> jsonNode) {
        this.inputHeaders = inputHeaders;
        this.line = line;
        this.previousLine = previousLine;
        this.previousMappedLine = previousMappedLine;
        this.mappings = map;
        this.primaryKeys = primaryKeys;
        this.lineNumber = lineNumber;
        this.filteredLineNumber = filteredLineNumber;
        this.mapLineConsumer = mapLineConsumer;
        this.outputHeaders = outputHeaders;
        this.defaultValues = defaultValues;
        this.jsonNode = jsonNode;
        this.valueCounts = valueCounts;
    }

    public List<String> getInputHeaders() {
        return inputHeaders;
    }

    public List<String> getLine() {
        return line;
    }

    public List<String> getPreviousLine() {
        return previousLine;
    }

    public List<String> getPreviousMappedLine() {
        return previousMappedLine;
    }

    public List<ValueMapping> getMappings() {
        return mappings;
    }

    public JDefaultDict<String, Set<String>> getPrimaryKeys() {
        return primaryKeys;
    }

    public JDefaultDict<String, JDefaultDict<String, AtomicInteger>> getValueCounts() {
        return valueCounts;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public int getFilteredLineNumber() {
        return filteredLineNumber;
    }

    public BiConsumer<List<String>, List<String>> getMapLineConsumer() {
        return mapLineConsumer;
    }

    public List<String> getOutputHeaders() {
        return outputHeaders;
    }

    public Map<String, String> getDefaultValues() {
        return defaultValues;
    }

    public Optional<JsonNode> getJsonNode() {
        return jsonNode;
    }

}
