package pl.allegro.search.solr.qparser;

public class ConfigWithQuery {
    String config;
    String query;

    public ConfigWithQuery(String config, String query) {
        this.config = config;
        this.query = query;
    }

    static ConfigWithQuery configWithQuery(String config, String query) {
        return new ConfigWithQuery(config, query);
    }
}
