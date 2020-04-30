package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class SortFieldsCompare {
    final private int numClauses;
    final private int[] reverseMul;
    final private FieldComparator[] fieldComparators;
    final private LeafFieldComparator[] leafFieldComparators;

    public Object[][] groupHeadValues; // growable

    /**
     * Constructs an instance based on the the (raw, un-rewritten) SortFields to be used,
     * and an initial number of expected groups (will grow as needed).
     */
    public SortFieldsCompare(SortField[] sorts, int initNumGroups) {
        numClauses = sorts.length;
        fieldComparators = new FieldComparator[numClauses];
        leafFieldComparators = new LeafFieldComparator[numClauses];
        reverseMul = new int[numClauses];
        for (int clause = 0; clause < numClauses; clause++) {
            SortField sf = sorts[clause];
            // we only need one slot for every comparator
            fieldComparators[clause] = sf.getComparator(1, clause);
            reverseMul[clause] = sf.getReverse() ? -1 : 1;
        }
        groupHeadValues = new Object[initNumGroups][];
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
        for (int clause = 0; clause < numClauses; clause++) {
            leafFieldComparators[clause] = fieldComparators[clause].getLeafComparator(context);
        }
    }

    public void setScorer(Scorable s) throws IOException {
        for (int clause = 0; clause < numClauses; clause++) {
            leafFieldComparators[clause].setScorer(s);
        }
    }

    // LUCENE-6808 workaround
    private static Object cloneIfBytesRef(Object val) {
        if (val instanceof BytesRef) {
            return BytesRef.deepCopyOf((BytesRef) val);
        }
        return val;
    }

    /**
     * Returns the current SortField values for the specified collapseKey.
     * If this collapseKey has never been seen before, then an array of null values is inited
     * and tracked so that the caller may update it if needed.
     */
    private Object[] getOrInitGroupHeadValues(int collapseKey) {
        Object[] values = groupHeadValues[collapseKey];
        if (null == values) {
            values = new Object[numClauses];
            groupHeadValues[collapseKey] = values;
        }
        return values;
    }

    /**
     * Records the SortField values for the specified contextDoc as the "best" values
     * for the group identified by the specified collapseKey.
     * <p>
     * Should be called the first time a contextKey is encountered.
     */
    public void setGroupValues(int collapseKey, int contextDoc) throws IOException {
        assert 0 <= collapseKey : "negative collapseKey";
        assert collapseKey < groupHeadValues.length : "collapseKey too big -- need to grow array?";
        setGroupValues(getOrInitGroupHeadValues(collapseKey), contextDoc);
    }

    /**
     * Records the SortField values for the specified contextDoc into the
     * values array provided by the caller.
     */
    private void setGroupValues(Object[] values, int contextDoc) throws IOException {
        for (int clause = 0; clause < numClauses; clause++) {
            leafFieldComparators[clause].copy(0, contextDoc);
            values[clause] = cloneIfBytesRef(fieldComparators[clause].value(0));
        }
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing group head
     * values for the group identified by the specified collapseKey, and overwrites them
     * (and returns true) if this document should become the new group head in accordance
     * with the SortFields
     * (otherwise returns false)
     */
    public boolean testAndSetGroupValues(int collapseKey, int contextDoc) throws IOException {
        assert 0 <= collapseKey : "negative collapseKey";
        assert collapseKey < groupHeadValues.length : "collapseKey too big -- need to grow array?";
        return testAndSetGroupValues(getOrInitGroupHeadValues(collapseKey), contextDoc);
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing values
     * array, and overwrites them (and returns true) if this document is the new group head in
     * accordance with the SortFields.
     * (otherwise returns false)
     */
    private boolean testAndSetGroupValues(Object[] values, int contextDoc) throws IOException {
        Object[] stash = new Object[numClauses];
        int lastCompare = 0;
        int testClause = 0;
        for (/* testClause */; testClause < numClauses; testClause++) {
            leafFieldComparators[testClause].copy(0, contextDoc);
            FieldComparator fcomp = fieldComparators[testClause];
            stash[testClause] = cloneIfBytesRef(fcomp.value(0));
            lastCompare = reverseMul[testClause] * fcomp.compareValues(stash[testClause], values[testClause]);

            if (0 != lastCompare) {
                // no need to keep checking additional clauses
                break;
            }
        }

        if (0 <= lastCompare) {
            // we're either not competitive, or we're completely tied with another doc that's already group head
            // that's already been selected
            return false;
        } // else...

        // this doc is our new group head, we've already read some of the values into our stash
        testClause++;
        System.arraycopy(stash, 0, values, 0, testClause);
        // read the remaining values we didn't need to test
        for (int copyClause = testClause; copyClause < numClauses; copyClause++) {
            leafFieldComparators[copyClause].copy(0, contextDoc);
            values[copyClause] = cloneIfBytesRef(fieldComparators[copyClause].value(0));
        }
        return true;
    }

    /**
     * Grows all internal arrays to the specified minSize
     */
    public void grow(int minSize) {
        groupHeadValues = ArrayUtil.grow(groupHeadValues, minSize);
    }
}
