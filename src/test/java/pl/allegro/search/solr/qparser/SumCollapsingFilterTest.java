package pl.allegro.search.solr.qparser;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getContext;
import static pl.allegro.search.solr.qparser.ConfigWithQuery.configWithQuery;

public class SumCollapsingFilterTest extends SolrTestCaseJ4 {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static List<String> COLLAPSE_FIELD = Lists.newArrayList("variant", "variant_hash");

    private static List<ConfigWithQuery> CONFIG_WITH_QUERY = Lists.newArrayList(
            configWithQuery("solrconfig.xml", "{!fastCollapse field=variant}*:*"),
            configWithQuery("solrconfig.xml", "{!fastCollapse field=variant_hash}*:*")
    );

    private IndexingUtility index;

    @Before
    public void setup()  {
        log.info("seed: " + getContext().getRunnerSeedAsString());
    }

    @Test
    public void shouldCollapseVariantsFromDifferentSegments()  {
        COLLAPSE_FIELD.forEach(field ->
                CONFIG_WITH_QUERY.forEach(cq ->
                {
                    //given
                    initCore(cq.config);
                    index = new IndexingUtility(h);
                    index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 10);
                    for (int j = 0; j < 5; j++)
                        for (int i = 0; i < 5; i++) {
                            index.indexDocumentAndCommit(null, "koń", "1234567" + i, 5);
                        }

                    //expect
                    assertQ(req("q", "*:*", "fq", cq.query),
                            "*[count(//doc)=6]");

                    //clean
                    deleteCore();
                }));
    }


    @Test
    public void shouldCollectDocumentInVariantInOrderOfApperance()  {
        CONFIG_WITH_QUERY.forEach(cq ->
        {
            //given
            initCore(cq.config);
            index = new IndexingUtility(h);
            index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 10);
            index.indexDocumentAndCommit(101, "koń", "1234567", 5);
            index.indexDocumentAndCommit(102, "antylopa", "1234567", 5);

            //expect
            assertQ(req("q", "*:*", "fq", cq.query),
                    "*[count(//doc)=2]",
                    "((//str[@name='id'])[1])/text()=1",
                    "((//str[@name='id'])[2])/text()=101");

            //clean
            deleteCore();
        });
    }

    @Test
    public void shouldCollectDocumentsFromDifferentVariants()  {
        CONFIG_WITH_QUERY.forEach(cq ->
        {
            //given
            initCore(cq.config);
            index = new IndexingUtility(h);
            index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 10);

            index.indexDocumentAndCommit(101, "antylopa", "1234567", 5);
            index.indexDocumentAndCommit(102, "koń", "1234567", 5);
            index.indexDocumentAndCommit(103, "zebra", "1234567", 10);
            index.indexDocumentAndCommit(201, "James Bond", "007", 4);
            index.indexDocumentAndCommit(202, "Jason Bourne", "007", 4);
            index.indexDocumentAndCommit(203, "Ethan Hunt", "007", 10);

            // expect
            assertQ(req("q", "*:*", "fq", cq.query),
                    "*[count(//doc)=3]",
                    "((//str[@name='id'])[text()=1])",
                    "((//str[@name='variant'])[text()=1234567])",
                    "((//str[@name='variant'])[text()=007])");
;
            //clean
            deleteCore();
        });
    }

    @Test
    public void shouldCollectBestDocumentsWhenSomeDocumentsWithCollapsingFieldIsNull()  {
        CONFIG_WITH_QUERY.forEach(cq ->
        {

            //given
            initCore(cq.config);
            index = new IndexingUtility(h);
            index.indexDocumentAndCommit(null, "pojedynczy dokument", null, 10);
            index.indexDocumentAndCommit(101, "antylopa", "1234567", 5);
            index.indexDocumentAndCommit(102, "koń", "1234567", 5);
            index.indexDocumentAndCommit(103, "zebra", "1234567", 10);
            index.indexDocumentAndCommit(201, "Marshall", null, 4);
            index.indexDocumentAndCommit(202, "Rubble", null, 4);
            index.indexDocumentAndCommit(203, "Chase", null, 10);

            // expect
            assertQ(req("q", "*:*", "fq", cq.query),
                    "*[count(//doc)=5]",
                    "((//str[@name='variant'])[text()=1234567])",
                    "((//str[@name='id'])[text()=1])",
                    "((//str[@name='id'])[text()=201])",
                    "((//str[@name='id'])[text()=202])",
                    "((//str[@name='id'])[text()=203])");
            //clean
            deleteCore();
        });
    }

    public static void initCore(String config) {
        try {
            initCore(config, "schema.xml", Files.createTempDir().getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
