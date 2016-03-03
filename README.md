# Summariser and Mapper for CSV and Access files

CSV summariser to quickly find out what you need to know about that random CSV file you were given, and a mapper to make the CSV file fit with what you need. It also provides a mapper for Access databases to create CSV files.

[![Build Status](https://travis-ci.org/ansell/csvsum.svg?branch=master)](https://travis-ci.org/ansell/csvsum) [![Coverage Status](https://coveralls.io/repos/ansell/csvsum/badge.svg?branch=master)](https://coveralls.io/r/ansell/csvsum?branch=master)

# Setup

Install Maven and Git

Download the Git repository.

Set the csvsum and csvmap programs to be executable.

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

The Language field in the mapping CSV file is one of: Javascript, Groovy, Lua.

The Mapping field in the mapping CSV file is either empty or a script in the Language.

If the Mapping field is empty, or is "inputValue", then the interpreters are not run for this field, and the values from the OldField are directly sent to NewField.

If the Mapping field contains a script, it is executed in the context of the following named parameters:

* inputHeaders : The headers from the input file as a list, in the order they were in the original file.
* inputField : The field that was designated to be the source for this mapping. Note that this parameter doesn't need to be used, and arbitrary fields can be merged using inputheaders and line.
* inputValue : The value of the field from inputField.
* outputField : The label of the output field that the results of this row will be assigned to.
* line : The values from the current line being processed from the input file, in the same order as the inputHeaders list.
* mapLine : The mapped values for the current line so far

The scripts also have access to two helper functions to get a specific column and to filter an entire line:

* col : Called using the syntax col('columnName'), and returns the value for that column on the current line
* outCol : Called using the syntax outCol('outputColumnName'), and returns the value that has been mapped so far for that column on the current line.
* filter : Called using the syntax filter(), and will make the line not appear in the results and short-circuit processing of the line for mapping purposes

# Access Mapper

## Usage

Run accessmap with --help to get usage details:

    ./accessmap --help

Run accessmap with a sample access file:

    ./accessmap --input src/test/resources/com/github/ansell/csvaccess/test-source.accdb --mapping src/test/resources/com/github/ansell/csvaccess/test-mapping.csv --output target/ --debug true

# Maven

    <dependency>
        <groupId>com.github.ansell.csv.sum</groupId>
        <artifactId>csvsum</artifactId>
        <version>0.0.4</version>
    </dependency>

# Changelog

## 2016-03-04
* Add helper function for getting the mapped value for a column in the current line, "outCol('outputColumnName')"

## 2016-03-02
* Add ability to filter lines in Javascript using a new function, "filter()"
* Add helper function for getting the value for a column in the current line, "col('columnName')"
* Add parallel processing of Access lines in different threads to take advantage of multiple cores available
* Write lines for Access using a different thread to reduce bottlenecks in the main thread

## 2016-02-24
* Add mapper for Microsoft Access databases to CSV files
* Output dates from Access to ISO8601 instead of the Jackcess/Java default

## 2016-02-17
* Release 0.0.4

## 2016-02-16
* Refactor package names to separate them
* Add requirement for javascript and lua mappings to include the return keyword in the mapping

## 2016-02-11
* Release 0.0.3
* Add CSVMapper to map CSV files to other CSV files using a given map
* Support Javascript (Nashorn), Groovy and Lua as scripting languages for the mapping

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
