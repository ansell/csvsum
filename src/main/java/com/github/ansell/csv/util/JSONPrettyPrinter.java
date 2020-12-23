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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Parses JSON documents and prints them using a pretty-printer
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class JSONPrettyPrinter {

    /**
     * Private constructor for static only class
     */
    private JSONPrettyPrinter() {
    }

    public static void main(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final OptionSpec<Void> help = parser.accepts("help").forHelp();
        final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class)
                .required().describedAs("The input JSON file to be summarised.");
        final OptionSpec<File> output = parser.accepts("output").withRequiredArg()
                .ofType(File.class)
                .describedAs("The output file, or the console if not specified.");
        final OptionSpec<Boolean> debug = parser.accepts("debug").withRequiredArg()
                .ofType(Boolean.class).defaultsTo(Boolean.FALSE)
                .describedAs("Set to true to debug.");

        OptionSet options = null;

        try {
            options = parser.parse(args);
        } catch (final OptionException e) {
            System.out.println(e.getMessage());
            parser.printHelpOn(System.out);
            throw e;
        }

        if (options.has(help)) {
            parser.printHelpOn(System.out);
            return;
        }

        final Path inputPath = input.value(options).toPath();
        if (!Files.exists(inputPath)) {
            throw new FileNotFoundException(
                    "Could not find input CSV file: " + inputPath.toString());
        }

        final Writer writer;
        if (options.has(output)) {
            writer = Files.newBufferedWriter(output.value(options).toPath());
        } else {
            writer = new BufferedWriter(new OutputStreamWriter(System.out));
        }

        final boolean debugBoolean = debug.value(options);

        final ObjectMapper inputMapper = new ObjectMapper();

        try (final BufferedReader newBufferedReader = Files.newBufferedReader(inputPath);
                JsonParser baseParser = inputMapper.getFactory().createParser(newBufferedReader);
                JsonGenerator generator = inputMapper.getFactory().createGenerator(writer)
                        .useDefaultPrettyPrinter();) {
            while (baseParser.nextToken() != null) {
                generator.copyCurrentEvent(baseParser);
            }
        } finally {
            writer.close();
        }
    }

}
