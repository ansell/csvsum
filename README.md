# CSV Summariser and Mapper

CSV summariser to quickly find out what you need to know about that random CSV file you were given, and a mapper to make the CSV file fit with what you need

[![Build Status](https://travis-ci.org/ansell/csvsum.svg?branch=master)](https://travis-ci.org/ansell/csvsum) [![Coverage Status](https://coveralls.io/repos/ansell/csvsum/badge.svg?branch=master)](https://coveralls.io/r/ansell/csvsum?branch=master)

# Setup

1. Install Maven
2. Set the csvsum program to be executable.

    chmod a+x ./csvsum
    chmod a+x ./csvmap

# CSV Summariser

## Usage

Run csvsum with --help to get usage details:

    ./csvsum --help

Run csvsum with a sample csv file:

    ./csvsum --input src/test/resources/com/github/ansell/csvsum/test-single-header-one-line.csv

# CSV Mapper

## Usage

Run csvmap with --help to get usage details:

    ./csvmap --help

Run csvmap with a sample csv file:

    ./csvmap --input src/test/resources/com/github/ansell/csvmap/test-source.csv --mapping src/test/resources/com/github/ansell/csvmap/test-mappings.csv

# Maven

    <dependency>
        <groupId>com.github.ansell.csvsum</groupId>
        <artifactId>csvsum</artifactId>
        <version>0.0.2</version>
    </dependency>

# Changelog

## 2016-02-11
* Add CSVMapper to map CSV files to other CSV files using a given map
* Support Javascript (Nashorn) and Groovy as languages
* Add infrastructure for Lua but no working mapping yet

## 2016-02-10
* Add possiblePrimaryKey to output based on if there are unique values found for each row for a field
* Add possibleInteger and possibleFloatingPoint checks for fields which have values and that look like integers or floating point values
* Add --samples=-1 to emit all values
* Improve memory efficiency for large sample value dumps using shared StringBuilder and lamda

## 2016-02-08

* Release 0.0.2
* Add --output to output to a CSV file
* Output summary to CSV file
* Add --samples to specify the number of sample values to include for each field
