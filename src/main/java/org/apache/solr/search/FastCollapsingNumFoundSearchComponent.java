package org.apache.solr.search;

import org.apache.lucene.search.TotalHits;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.response.BasicResultContext;

public class FastCollapsingNumFoundSearchComponent extends SearchComponent {

    public static final String COLLAPSING_REQUEST_TOTAL_HITS = "collapsing_total_hits";
    public static final String HITS = "hits";

    @Override
    public void prepare(ResponseBuilder rb) {

    }

    @Override
    public void process(ResponseBuilder rb) {
        if (rb.req.getContext().containsKey(COLLAPSING_REQUEST_TOTAL_HITS)) {
            BasicResultContext response = (BasicResultContext) rb.rsp.getResponse();
            DocSlice docList = (DocSlice) response.getDocList();
            Integer hits = (Integer) rb.req.getContext().get(COLLAPSING_REQUEST_TOTAL_HITS);
            DocSlice modifiedDocList = new DocSlice(docList.offset, docList.len, docList.docs, docList.scores, hits, docList.maxScore, TotalHits.Relation.EQUAL_TO);
            rb.rsp.getValues().remove("response");
            rb.rsp.addResponse(new BasicResultContext(modifiedDocList, response.getReturnFields(), response.getSearcher(), response.getQuery(), response.getRequest()));
            logResponse(rb, hits);
        }
    }

    private void logResponse(ResponseBuilder rb, Integer hits) {
        rb.rsp.getToLog().remove(HITS);
        rb.rsp.getToLog().add(HITS, hits);
    }


    @Override
    public String getDescription() {
        return "A component that processes search response updating its numFound if collapsing filter has changed it.";
    }
}
