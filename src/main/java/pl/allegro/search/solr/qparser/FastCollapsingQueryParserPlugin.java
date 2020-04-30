package pl.allegro.search.solr.qparser;


import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.FastCollapsingQueryFilter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SortSpecParsing;


public class FastCollapsingQueryParserPlugin extends QParserPlugin {

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        String field = localParams.get("field");
        FieldType fieldType = req.getSchema().getFieldType(field);
        boolean isExactCount = localParams.getBool("exactCount", false);
        return new QParser(qstr, localParams, params, req) {
            @Override
            public Query parse() {
                //if null sort to , Sort.INDEXORDER
                Sort sort = SortSpecParsing.parseSortSpec(params.get(CommonParams.SORT), req).getSort();
                int rows = params.getInt("rows", 10);
                int start = params.getInt("start", 0);

                if (isNoSortingQuery(sort, rows)) {
                    return new SumCollapsingQueryFilter(field, fieldType);
                }
                if (!isCursorQuery(params)) {
                            return new FastCollapsingQueryFilter(
                                    field, fieldType,
                                    sort,
                                    rows + start,
                                    isExactCount,
                                    req.getContext());
                }
                return new CollapsingQueryFilter(field, fieldType, sort);
            }
        };
    }

    private boolean isNoSortingQuery(Sort sort, int rows) {
        return sort == null || rows == 0;
    }

    private boolean isCursorQuery(SolrParams params) {
        return !params.get("cursorMark", "").isEmpty() ||
                !params.get("nextCursorMark", "").isEmpty();
    }

    @Override
    public void init(NamedList args) {
        super.init(args);
    }

}
