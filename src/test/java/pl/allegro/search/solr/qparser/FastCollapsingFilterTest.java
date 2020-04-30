package pl.allegro.search.solr.qparser;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getContext;

public class FastCollapsingFilterTest extends SolrTestCaseJ4 {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private pl.allegro.search.solr.qparser.IndexingUtility index;

    private static List<String> COLLAPSE_FIELD = Lists.newArrayList("variant", "variant_hash");

    private String FILTER_QUERY = "{!fastCollapse field=%s}*:*";

    @Before
    public void setup() throws Exception {
        log.info("seed: " + getContext().getRunnerSeedAsString());
        initCore("solrconfig.xml", "schema.xml", Files.createTempDir().getAbsolutePath());
        index = new IndexingUtility(h);
    }

    @After
    public void close() throws Exception {
        deleteCore();
    }


    @Test
    public void shouldCollectBestDocumentsInOrderOfAppearanceIfCriteriaAreEqual() {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 5);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[text()=1])",
                        "((//str[@name='id'])[text()=101])")
        );
    }

    @Test
    public void shouldCollectBestDocumentsWhenIndexedDocumentAreAlreadySorted()  {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 5);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 6);

        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc", "rows", "10"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=101"));
    }

    @Test
    public void shouldCollectBestDocumentsWhenIndexedDocumentNotSorted() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 5);
        index.indexDocumentAndCommit(101, "koń", "1234567", 6);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);

        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc", "rows", "10"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=102"));
    }

    @Test
    public void shouldCollectBestDocumentsInAscSorting() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 11);
        index.indexDocumentAndCommit(101, "koń", "1234567", 15);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 10);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[1])/text()=102",
                        "((//str[@name='id'])[2])/text()=1")
        );
    }

    @Test
    public void shouldCollectBestDocumentsInDescSorting() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 20);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 10);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price desc"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=102")
        );
    }


    @Test
    public void shouldCollectBestDocumentsInSortingByTwoFields() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 1);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);
        index.indexDocumentAndCommit(103, "zebra", "1234567", 10);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc, name asc"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=102")
        );
    }

    @Test
    public void shouldCollectBestDocumentsInSortingByTwoFieldsWithMixedOrder() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 1);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);
        index.indexDocumentAndCommit(103, "zebra", "1234567", 10);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc, name desc"),
                        "*[count(//doc)=2]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=101"));

    }

    @Test
    public void shouldCollectBestDocumentsFromDifferentVariants() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 1);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);
        index.indexDocumentAndCommit(103, "zebra", "1234567", 10);
        index.indexDocumentAndCommit(201, "James Bond", "007", 4);
        index.indexDocumentAndCommit(202, "Jason Bourne", "007", 4);
        index.indexDocumentAndCommit(203, "Ethan Hunt", "007", 10);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc, name desc"),
                        "*[count(//doc)=3]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=202",
                        "((//str[@name='id'])[3])/text()=101"));
    }

    @Test
    public void shouldCollectBestDocumentsWhenSomeDocumentsWithCollapsingFieldIsNull() throws Exception {
        //given
        index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 1);
        index.indexDocumentAndCommit(101, "koń", "1234567", 5);
        index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);
        index.indexDocumentAndCommit(103, "zebra", "1234567", 10);
        index.indexDocumentAndCommit(201, "Marshall", null, 4);
        index.indexDocumentAndCommit(202, "Rubble", null, 4);
        index.indexDocumentAndCommit(203, "Chase", null, 10);

        // expect
        COLLAPSE_FIELD.forEach(field ->
                assertQ(req("q", "*:*", "fq", String.format(FILTER_QUERY, field), "sort", "price asc, name desc"),
                        "*[count(//doc)=5]",
                        "((//str[@name='id'])[1])/text()=1",
                        "((//str[@name='id'])[2])/text()=202",
                        "((//str[@name='id'])[3])/text()=201",
                        "((//str[@name='id'])[4])/text()=101",
                        "((//str[@name='id'])[5])/text()=203"));
    }

}
