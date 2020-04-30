package pl.allegro.search.solr.qparser;

import org.apache.solr.util.TestHarness;

import java.util.concurrent.atomic.AtomicInteger;

class IndexingUtility {

    private TestHarness h;
    private AtomicInteger idGenerator = new AtomicInteger();

    IndexingUtility(TestHarness h) {
        this.h = h;
    }

    void indexDocumentAndCommit(Integer id, String name, String variant, int price) {
        indexDocument(id, name, variant, price);
        commit();
    }

    void indexDocument(Integer id, String name, String variant, int price) {
        h.update("<add>\n" +
                "  <doc>\n" +
                "    <field name=\"id\">" + getIndexingId(id) + "</field>\n" +
                "    <field name=\"name\">" + name + "</field>\n" +
                ((variant != null)? indexVariant(variant) :"") +
                "    <field name=\"price\">" + price + "</field>\n" +
                "  </doc>\n" +
                "</add>\n" +
                "");
    }

    private String indexVariant(String variant) {
        return "    <field name=\"variant\">" + variant + "</field>\n" +
                "    <field name=\"variant_hash\">" + Long.parseLong(variant) + "</field>\n";
    }

    void commit() {
        h.update("<add>\n" +
                "<commit/>" +
                "</add>\n" +
                "");
    }


    private int getIndexingId(Integer id) {
        return id == null?idGenerator.incrementAndGet():id;
    }
}
