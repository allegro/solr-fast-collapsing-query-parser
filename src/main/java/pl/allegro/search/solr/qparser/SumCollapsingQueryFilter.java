package pl.allegro.search.solr.qparser;

import com.carrotsearch.hppc.LongHashSet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;

import java.io.IOException;
import java.util.Objects;

import static pl.allegro.search.solr.qparser.SortedDocValuesHash.EMPTY;

public class SumCollapsingQueryFilter extends ExtendedQueryBase implements PostFilter {

    private final String field;
    private final FieldType fieldType;


    public SumCollapsingQueryFilter(String field, FieldType fieldType) {
        this.field = field;
        this.fieldType = fieldType;
    }

    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
        return new DelegatingCollector() {
            private SortedDocValuesHash sortedDocValuesHash;
            private long previousHash;
            private boolean firstDocumentInSegment;

            private LongHashSet alreadyCollapsed = new LongHashSet();


            @Override
            public void collect(int docNumber) throws IOException {
                final long collapsedFieldHash = sortedDocValuesHash.getHash(docNumber);

                if (collapsedFieldHash == EMPTY) {
                    super.collect(docNumber);
                } else {
                    if (firstDocumentInSegment || collapsedFieldHash != previousHash) {
                        if (!alreadyCollapsed.contains(collapsedFieldHash)) {
                            super.collect(docNumber);
                            alreadyCollapsed.add(collapsedFieldHash);
                        }
                    }
                }
                previousHash = collapsedFieldHash;
                firstDocumentInSegment = false;
            }

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                super.doSetNextReader(context);
                sortedDocValuesHash = new SortedDocValuesHash(context.reader(), field, fieldType);
                firstDocumentInSegment = true;
            }
        };
    }

    @Override
    public int getCost() {
        return Math.max(super.getCost(), 100);
    }

    @Override
    public boolean getCache() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SumCollapsingQueryFilter)) {
            return false;
        }
        SumCollapsingQueryFilter that = (SumCollapsingQueryFilter) o;
        return Objects.equals(this.field,that.field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }
}
