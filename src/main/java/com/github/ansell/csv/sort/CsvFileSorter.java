package com.github.ansell.csv.sort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.DataWriter;
import com.fasterxml.sort.DataWriterFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;

class CsvFileSorter<T> extends Sorter<T> {
    public CsvFileSorter(Class<T> entryType, SortConfig config, CsvMapper mapper, CsvSchema schema,
            Comparator<T> comparator) throws IOException {
        super(config, new ReaderFactory<T>(mapper.readerFor(mapper.constructType(entryType)),
                mapper, schema), new WriterFactory<T>(mapper, schema), comparator);
    }

    static class ReaderFactory<R> extends DataReaderFactory<R> {
        private final ObjectReader _reader;
        private final CsvSchema _schema;
        private final CsvMapper _mapper;

        public ReaderFactory(ObjectReader r, CsvMapper mapper, CsvSchema schema) {
            _reader = r;
            _schema = schema;
            _mapper = mapper;
        }

        @Override
        public DataReader<R> constructReader(InputStream in) throws IOException {
            final MappingIterator<R> it = _reader.with(_schema).readValues(in);
            return new Reader<R>(it);
        }
    }

    static class Reader<E> extends DataReader<E> {
        protected final MappingIterator<E> _iterator;

        public Reader(MappingIterator<E> it) {
            _iterator = it;
        }

        @Override
        public E readNext() throws IOException {
            if (_iterator.hasNext()) {
                return _iterator.nextValue();
            }
            return null;
        }

        @Override
        public int estimateSizeInBytes(E item) {
            // Not empirically determined, just a guess
            return 150;
        }

        @Override
        public void close() throws IOException {
            // auto-closes when we reach end
        }
    }

    static class WriterFactory<W> extends DataWriterFactory<W> {
        protected final ObjectMapper _mapper;
        private final CsvSchema _schema;

        public WriterFactory(CsvMapper m, CsvSchema schema) {
            _mapper = m;
            _schema = schema;
        }

        @Override
        public DataWriter<W> constructWriter(OutputStream out) throws IOException {
            return new Writer<W>(_mapper, out, _schema);
        }
    }

    static class Writer<E> extends DataWriter<E> {
        protected final ObjectMapper _mapper;
        protected final JsonGenerator _generator;

        public Writer(ObjectMapper mapper, OutputStream out, CsvSchema schema) throws IOException {
            _mapper = mapper;
            _generator = _mapper.getFactory().createGenerator(out);
            _generator.setSchema(schema);
        }

        @Override
        public void writeEntry(E item) throws IOException {
            _mapper.writeValue(_generator, item);
        }

        @Override
        public void close() throws IOException {
            _generator.close();
        }
    }
}
