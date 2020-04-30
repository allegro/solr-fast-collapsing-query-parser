package org.apache.lucene.search;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongScatterSet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.SolrIndexSearcher;
import pl.allegro.search.solr.qparser.SortedDocValuesHash;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.solr.search.FastCollapsingNumFoundSearchComponent.COLLAPSING_REQUEST_TOTAL_HITS;
import static pl.allegro.search.solr.qparser.SortedDocValuesHash.EMPTY;

public class FastCollapsingQueryFilter extends ExtendedQueryBase implements PostFilter {

    private final String field;
    private final FieldType fieldType;
    private final Sort sort;
    private final int queueSize;
    private final boolean isExactCount;
    private final Map<Object, Object> reqContext;

    class EntryWithHash extends FieldValueHitQueue.Entry {

        long hash;

        public EntryWithHash(int slot, int doc, float score, long hash) {
            super(slot, doc);
            this.hash = hash;
            this.score = score;
        }
    }

    public FastCollapsingQueryFilter(String field, FieldType fieldType, Sort sort, int queueSize, boolean isExactCount, Map<Object, Object> reqContext) {
        this.field = field;
        this.fieldType = fieldType;
        this.sort = sort;
        this.queueSize = queueSize;
        this.isExactCount = isExactCount;
        this.reqContext = reqContext;
    }

    @Override
    public DelegatingCollector getFilterCollector(final IndexSearcher indexSearcher) {
        SolrIndexSearcher searcher = (SolrIndexSearcher) indexSearcher;
        int leafCount = searcher.getTopReaderContext().leaves().size();

        try {
            SortField[] rewrittenSortFields = rewriteSort(sort, searcher).getSort();
            FieldValueHitQueue<EntryWithHash> queue = FieldValueHitQueue.create(rewrittenSortFields, queueSize);
            return new DelegatingCollector() {
                private SortedDocValuesHash sortedDocValuesHash;
                private LeafReaderContext[] contexts = new LeafReaderContext[leafCount];
                private Map<Long, EntryWithHash> variantHashToElement = new HashMap<>(queueSize);
                private LongIntHashMap variantHashToId = new LongIntHashMap(queueSize);
                private IntArrayDeque freeVariantIds = new IntArrayDeque(queueSize);
                //we start with index =1 and we need one more to compare before removing, so that why we have plus two
                private final SortFieldsCompare variantComparator = new SortFieldsCompare(rewrittenSortFields, queueSize + 2);
                private int totalHits;
                private boolean queueFull;
                private LeafFieldComparator queueComparator;
                private int reverseMul;
                EntryWithHash bottomElement = null;
                private LongScatterSet numFoundVariant = isExactCount ? new LongScatterSet(128) : null;
                private int numFoundNonVariant = 0;

                @Override
                protected void doSetNextReader(LeafReaderContext context) throws IOException {
                    super.doSetNextReader(context);
                    sortedDocValuesHash = new SortedDocValuesHash(context.reader(), field, fieldType);
                    this.contexts[context.ord] = context;
                    initContextComparators(context);
                }

                private void initContextComparators(LeafReaderContext context) throws IOException {
                    variantComparator.setNextReader(context);
                    if (queue.getComparators(context).length == 1) {
                        this.reverseMul = queue.getReverseMul()[0];
                        this.queueComparator = queue.getComparators(context)[0];
                    } else {
                        this.reverseMul = 1;
                        this.queueComparator = new MultiLeafFieldComparator(queue.getComparators(context), queue.getReverseMul());
                    }
                }

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    this.variantComparator.setScorer(scorer);
                    queueComparator.setScorer(scorer);
                }

                private void countNumFound(long variantHash) {
                    if (isExactCount) {
                        if (variantHash != EMPTY) {
                            numFoundVariant.add(variantHash);
                        } else {
                            numFoundNonVariant++;
                        }
                    }
                }

                @Override
                public void collect(int docNumber) throws IOException {
                    int globalDoc = docNumber + this.docBase;

                    ++totalHits;
                    if (queueFull) {

                        if (documentIsWorseThenAllInQueue(docNumber)) {
                            if (isExactCount) {
                                countNumFound(readVariantHash(docNumber));
                            }
                            return;
                        }
                        long variantHash = readVariantHash(docNumber);
                        countNumFound(variantHash);


                        if (isVariantDocument(variantHash)) {
                            if (variantContainedInQueue(variantHash)) {
                                queueLengthIsSameSoReduceTotalHits();
                                if (currentDocumentIsBetterThenItsVariantInQueue(docNumber, variantHash)) {
                                    updateElementInMiddleInQueue(docNumber, globalDoc, variantHash);
                                    return;
                                } else {//current document is worse then previous variant
                                    return;
                                }
                            } else {
                                recordVariantDocument4Comparison(docNumber, variantHash);
                                variantHashToElement.put(variantHash, bottomElement);//exchange best variant
                            }
                        }


                        long bottomQueueVariantHash = bottomElement.hash;
                        if (isVariantDocument(bottomQueueVariantHash)) {
                            // remova last element from temporary structures
                            variantHashToElement.remove(bottomQueueVariantHash);
                            int variantHashId = getAndRemoveVariantHashId(bottomQueueVariantHash);
                            variantComparator.groupHeadValues[variantHashId] = null;
                        }
                        updateElementInQueue(bottomElement, docNumber, globalDoc, variantHash);
                    } else {
                        // Startup transient: queue hasn't gathered numHits yet
                        int slot = totalHits - 1;

                        long variantHash = readVariantHash(docNumber);
                        countNumFound(variantHash);
                        if (isVariantDocument(variantHash)) {
                            if (variantContainedInQueue(variantHash)) {
                                queueLengthIsSameSoReduceTotalHits();
                                if (currentDocumentIsBetterThenItsVariantInQueue(docNumber, variantHash)) {
                                    EntryWithHash elementToRemove = variantHashToElement.get(variantHash);
                                    queue.remove(elementToRemove);
                                    slot = elementToRemove.slot;
                                } else {//current document is worse then previous variant
                                    return;
                                }
                            } else {
                                recordVariantDocument4Comparison(docNumber, variantHash);
                            }

                        }

                        // Copy hit into queue
                        queueComparator.copy(slot, docNumber);
                        EntryWithHash entry = new EntryWithHash(slot, globalDoc, scorer.score(), variantHash);
                        if (isVariantDocument(variantHash)) {
                            variantHashToElement.put(variantHash, entry);
                        }
                        add(entry);
                    }
                }

                private void updateElementInMiddleInQueue(int docNumber, int globalDoc, long variantHash) throws IOException {
                    EntryWithHash elementToUpdate = variantHashToElement.get(variantHash);
                    queue.remove(elementToUpdate);
                    queueComparator.copy(elementToUpdate.slot, docNumber);
                    elementToUpdate.doc = globalDoc;
                    elementToUpdate.score = scorer.score();
                    bottomElement = queue.add(elementToUpdate);
                    queueComparator.setBottom(bottomElement.slot);
                }

                private long readVariantHash(int docNumber) throws IOException {
                    return sortedDocValuesHash.getHash(docNumber);
                }

                private void synchronizeQueueWithNewElement() throws IOException {
                    bottomElement = queue.updateTop();
                    queueComparator.setBottom(bottomElement.slot);
                }

                private void updateElementInQueue(EntryWithHash element, int docNumber, int globalDoc, long variantHash) throws IOException {
                    queueComparator.copy(element.slot, docNumber);
                    element.doc = globalDoc;
                    element.score = scorer.score();
                    element.hash = variantHash;
                    synchronizeQueueWithNewElement();
                }

                private int getAndRemoveVariantHashId(long bottomQueueVariantHash) {
                    int variantId = variantHashToId.remove(bottomQueueVariantHash);
                    freeVariantIds.addFirst(variantId);
                    return variantId;
                }

                private void recordVariantDocument4Comparison(int docNumber, long variantHash) throws IOException {
                    int variantId = initVariantIndex();
                    variantHashToId.put(variantHash, variantId);
                    variantComparator.setGroupValues(variantId, docNumber);
                }

                private boolean currentDocumentIsBetterThenItsVariantInQueue(int docNumber, long hash) throws IOException {
                    return variantComparator.testAndSetGroupValues(variantHashToId.get(hash), docNumber);
                }

                private void queueLengthIsSameSoReduceTotalHits() {
                    totalHits--;
                }

                private boolean variantContainedInQueue(long hash) {
                    return variantHashToElement.containsKey(hash);
                }

                private boolean documentIsWorseThenAllInQueue(int docNumber) throws IOException {
                    return reverseMul * queueComparator.compareBottom(docNumber) <= 0;
                }

                private int initVariantIndex() {
                    if (!freeVariantIds.isEmpty()) {
                        return freeVariantIds.removeFirst();
                    }
                    return nextId.incrementAndGet();
                }


                AtomicInteger nextId = new AtomicInteger();

                final void add(EntryWithHash entry) throws IOException {
                    bottomElement = queue.add(entry);
                    queueFull = queueSize == totalHits;
                    if (queueFull) {
                        queueComparator.setBottom(bottomElement.slot);
                    }

                }

                @Override
                public void finish() throws IOException {
                    if (queue.size() > 0) {
                        DummyScorer dummy = new DummyScorer();

                        ArrayList<EntryWithHash> entries = new ArrayList<EntryWithHash>(queue.size());
                        for (EntryWithHash entry : queue)
                            entries.add(entry);
                        entries.sort(Comparator.comparingInt(a -> a.doc));

                        int currentContext = 0;
                        int currentDocBase = 0;
                        leafDelegate = delegate.getLeafCollector(contexts[currentContext]);

                        for (EntryWithHash entry : entries) {
                            int docId = entry.doc;

                            if (docId >= getNextDocBase(currentContext)) {
                                while (docId >= getNextDocBase(currentContext)) {
                                    currentContext++;
                                    currentDocBase = contexts[currentContext].docBase;
                                }
                                leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
                            }

                            int contextDoc = docId - currentDocBase;
                            dummy.docId = contextDoc;
                            dummy.score = entry.score;
                            leafDelegate.setScorer(dummy);
                            leafDelegate.collect(contextDoc);
                        }

                        reqContext.put(COLLAPSING_REQUEST_TOTAL_HITS,
                                isExactCount ?
                                        numFoundNonVariant + numFoundVariant.size() : totalHits
                        );
                    }
                }

                private int getNextDocBase(int currentContext) {
                    return currentContext + 1 < contexts.length ? contexts[currentContext + 1].docBase : Integer.MAX_VALUE;
                }

            };
        } catch (IOException e) {
            throw new FastCollapsingFilterInitializationException(e);
        }
    }

    private boolean isVariantDocument(long hash) {
        return hash != EMPTY;
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
        if (!(o instanceof FastCollapsingQueryFilter)) {
            return false;
        }
        FastCollapsingQueryFilter that = (FastCollapsingQueryFilter) o;
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
        public float getMaxScore(int upTo) throws IOException {
            return Float.POSITIVE_INFINITY;
        }

    }

    private static Sort rewriteSort(Sort sort, IndexSearcher searcher) throws IOException {
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
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            return null;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

}
