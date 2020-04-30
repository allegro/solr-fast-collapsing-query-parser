package pl.allegro.search.solr.qparser;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IntValueFieldType;
import org.apache.solr.schema.LongValueFieldType;

import java.io.IOException;

public class SortedDocValuesHash {
    public static final int EMPTY = new BytesRef().hashCode();

    private final DocIdSetIterator docValues;
    private FieldType fieldType;

    public SortedDocValuesHash(LeafReader reader, String field, FieldType fieldType) throws IOException {
        this.fieldType = fieldType;
        if (fieldType instanceof LongValueFieldType || fieldType instanceof IntValueFieldType) {
                docValues = reader.getNumericDocValues(field);
        } else {
            docValues = reader.getSortedDocValues(field);
        }
    }

    public long getHash(int docNumber) throws IOException {
        if (docValues == null || docValues.docID() == Integer.MAX_VALUE || docValues.docID() > docNumber) {
            return EMPTY;
        }
        if (docValues.docID() == docNumber || docValues.advance(docNumber) == docNumber) {
            return getValue();
        }
        return EMPTY;
    }

    private long getValue() throws IOException {
        if (fieldType instanceof LongValueFieldType || fieldType instanceof IntValueFieldType) {
            return ((NumericDocValues) docValues).longValue();
        } else {
            return ((SortedDocValues) docValues).binaryValue().hashCode();
        }
    }
}
