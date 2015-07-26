package net.zawila;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClientManager.class);

    private final Client client;
    private final String index;
    private final String type;
    private final String field;

    public ClientManager(final String index, final String type, final String field, final String... hosts) {

        this.index = index;
        this.type = type;
        this.field = field;

        LOG.info("Creating new client manager.");
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "cluster").build();
        TransportClient client = new TransportClient(settings);

        for(String host : hosts) {
            LOG.info("Adding new '{}' host.", host);
            client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
        }

        this.client = client;

    }

    public long getDocumentNumberInPeriod(final DateTime from, final DateTime to){

        RangeQueryBuilder range = QueryBuilders.rangeQuery(field).gte(from).lt(to);

        CountResponse response = this.client.prepareCount(index).setTypes(type).setQuery(range).get();

        return response.getCount();
    }

    public void reindex(final DateTime from, final DateTime to, final String newIndexName){

        LOG.info("Start re-indexing for data between {} and {}", from, to);

        RangeQueryBuilder range = QueryBuilders.rangeQuery("timestamp").gte(from).lt(to);

        SearchResponse response = this.client.prepareSearch(index).setTypes(type).setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .addFields("_parent", "_source", "_timestamp")
                .setQuery(range)
                .setSize(500).get();

        int number = 0;

        while(true) {
            BulkRequestBuilder bulkRequest = this.client.prepareBulk();

            for(SearchHit hit : response.getHits()) {
                LOG.debug("Document retrieved {}",hit.getId());

                IndexRequestBuilder indexBuilder = this.client.prepareIndex().setIndex(newIndexName)
                        .setType(type).setId(hit.getId()).setSource(hit.getSource());

                Object parent = hit.getFields().get("parent");
                if(parent != null && parent instanceof  String) {
                    LOG.info("Added parent '{}' to document {}", parent, hit.getId());
                    indexBuilder.setParent(parent.toString());
                }

                Object timestamp = hit.getFields().get("_timestamp");
                if(timestamp != null && timestamp instanceof String){
                    LOG.info("Added timestamp '{}' to document {}", timestamp, hit.getId());
                    indexBuilder.setTimestamp(timestamp.toString());
                }

                bulkRequest.add(indexBuilder);
                number++;
            }

            if(response.getHits().getHits().length > 0){
                BulkResponse bulkResponse = bulkRequest.get();

                if(bulkResponse.hasFailures()){
                    LOG.error(bulkResponse.buildFailureMessage());
                    LOG.error("Problem with inserting data.");
                }
            }

            response = this.client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).get();

            if(response.getHits().getHits().length == 0) {
                break;
            }
        }

        LOG.info("Inserted {} documents", number);

    }

    public Client getClient() {
        return client;
    }
}