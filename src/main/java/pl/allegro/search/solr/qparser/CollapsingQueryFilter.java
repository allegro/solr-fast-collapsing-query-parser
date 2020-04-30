package pl.allegro.search.solr.qparser;

import com.carrotsearch.hppc.LongFloatHashMap;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortFieldsCompare;
import org.apache.lucene.search.Weight;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static pl.allegro.search.solr.qparser.SortedDocValuesHash.EMPTY;


public class CollapsingQueryFilter extends ExtendedQueryBase implements PostFilter {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final int INIT_COLLAPSED_SET_SIZE = 100;
    private final String field;
    private final FieldType fieldType;

    private final Sort sort;

    private long previousOrd;
    private boolean firstDocumentInSegment;

    public CollapsingQueryFilter(String field, FieldType fieldType, Sort sort) {
        this.field = field;
        this.fieldType = fieldType;
        this.sort = sort;
    }

    @Override
    public DelegatingCollector getFilterCollector(final IndexSearcher indexSearcher) {
        SolrIndexSearcher searcher = (SolrIndexSearcher) indexSearcher;
        int leafCount = searcher.getTopReaderContext().leaves().size();
        try {
            Sort rewrittenSort = rewriteSort(sort, searcher);
            return new DelegatingCollector() {
                private SortedDocValuesHash sortedDocValuesHash;
                private LeafReaderContext[] contexts = new LeafReaderContext[leafCount];
                private LongIntHashMap valuesDocIds = new LongIntHashMap(INIT_COLLAPSED_SET_SIZE);
                private LongIntHashMap valuesToIndex = new LongIntHashMap(INIT_COLLAPSED_SET_SIZE);
                private LongFloatHashMap scores = new LongFloatHashMap(INIT_COLLAPSED_SET_SIZE);
                private final SortFieldsCompare compareState = new SortFieldsCompare(rewrittenSort.getSort(), INIT_COLLAPSED_SET_SIZE);

                @Override
                protected void doSetNextReader(LeafReaderContext context) throws IOException {
                    super.doSetNextReader(context);
                    sortedDocValuesHash = new SortedDocValuesHash(context.reader(), field, fieldType);
                    this.contexts[context.ord] = context;
                    firstDocumentInSegment = true;
                    compareState.setNextReader(context);
                }

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    this.compareState.setScorer(scorer);
                }

                @Override
                public void collect(int docNumber) throws IOException {
                    int globalDoc = docNumber+this.docBase;

                    if (!firstDocumentInSegment && previousOrd == EMPTY) {
                        leafDelegate.setScorer(createScorer(docNumber));
                        super.collect(docNumber);
                        return;
                    }

                    long collapsedFieldHash = sortedDocValuesHash.getHash(docNumber);

                    if (collapsedFieldHash == EMPTY) {
                        leafDelegate.setScorer(createScorer(docNumber));
                        super.collect(docNumber);
                    }else{
                        if (valuesDocIds.containsKey(collapsedFieldHash)){
                            int variantIndex = valuesToIndex.get(collapsedFieldHash);
                            if (compareState.testAndSetGroupValues(variantIndex, docNumber)) {
                                valuesDocIds.put(collapsedFieldHash,globalDoc);
                                scores.put(collapsedFieldHash,scorer.score());
                            }
                        } else {
                            valuesDocIds.put(collapsedFieldHash,globalDoc);
                            int variantIndex = nextId.incrementAndGet();
                            valuesToIndex.put(collapsedFieldHash, variantIndex);
                            scores.put(collapsedFieldHash,scorer.score());
                            if (compareState.groupHeadValues.length <= variantIndex) {
                                compareState.grow(compareState.groupHeadValues.length *2);
                            }
                            compareState.setGroupValues(variantIndex, docNumber);
                        }
                    }
                    previousOrd = collapsedFieldHash;
                    firstDocumentInSegment = false;
                }
                AtomicInteger nextId = new AtomicInteger();

                @Override
                public void finish() throws IOException {
                    DummyScorer dummy = new DummyScorer();
                    for (LongIntCursor valuesDocId : valuesDocIds) {
                        int currentContext = 0;int currentDocBase = 0;
                        int nextDocBase = getNextDocBase(currentContext);
                        leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
                        int docId = valuesDocId.value;
                        while (docId >= nextDocBase) {
                            currentContext++;
                            currentDocBase = contexts[currentContext].docBase;
                            nextDocBase = getNextDocBase(currentContext);
                            leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
                        }
                        int contextDoc = docId - currentDocBase;
                        dummy.docId = contextDoc;
                        dummy.score = scores.get(valuesDocId.key);
                        leafDelegate.setScorer(dummy);
                        leafDelegate.collect(contextDoc);
                    }
                }

                private int getNextDocBase(int currentContext) {
                    return currentContext + 1 < contexts.length ? contexts[currentContext + 1].docBase : Integer.MAX_VALUE;
                }


                private Scorer createScorer(int docNumber) {
                    return new Scorer(new DummyWeight()) {
                        public float score() throws IOException {
                            return scorer.score();
                        }

                        public int docID() {
                            return docNumber;
                        }

                        public DocIdSetIterator iterator() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public float getMaxScore(int upTo) {
                            return Float.POSITIVE_INFINITY;
                        }

                    };
                }

            };
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
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
        if (!(o instanceof CollapsingQueryFilter)) {
            return false;
        }
        CollapsingQueryFilter that = (CollapsingQueryFilter) o;
        return Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }

    private static class DummyScorer extends Scorer {

        public float score;
        public int docId;

        public DummyScorer() {
            super(new DummyWeight());
        }

        public float score() {
            return score;
        }

        public int docID() {
            return docId;
        }

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getMaxScore(int upTo)  {
            return Float.POSITIVE_INFINITY;
        }
    }

    public static Sort rewriteSort(Sort sort, IndexSearcher searcher) throws IOException {
        assert null != sort : "Sort must not be null";
        assert null != searcher : "Searcher must not be null";
        return sort.rewrite(searcher);
    }

    private static class DummyWeight extends Weight {

        DummyWeight() {
            super(new MatchNoDocsQuery());
        }

        @Override
        @Deprecated
        public void extractTerms(Set<Term> terms) {
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) {
            return null;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

}
