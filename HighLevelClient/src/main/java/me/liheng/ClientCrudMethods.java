package me.liheng;

import me.liheng.model.CatalogItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClientCrudMethods {

    static Logger LOG = LogManager.getLogger(ClientCrudMethods.class);
    private final String index;
    private final RestHighLevelClient restClient;
    private final ObjectMapper objectMapper;

    public ClientCrudMethods(String index, RestHighLevelClient restClient) {
        this.index = index;
        this.restClient = restClient;
        this.objectMapper = new ObjectMapper();
    }

    protected String getIndex() {
        return index;
    }

    protected RestHighLevelClient getRestClient() {
        return restClient;
    }

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void createCatalogItem(List<CatalogItem> items) {
        items.stream().forEach(e-> {

            //index --> table to store documents
            //document --> record, json document
            //use IndexRequest and initialize it with the name of the desired index
            IndexRequest request = new IndexRequest(getIndex());
            try {
                //set the ID on the request and add JSON as a source
                request.id(""+e.getId());
                request.source(getObjectMapper().writeValueAsString(e), XContentType.JSON);

                request.timeout(TimeValue.timeValueSeconds(10));

                //Calling the high-level client index API with the request synchronously will return the index response
                IndexResponse indexResponse = getRestClient().index(request, RequestOptions.DEFAULT);

                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    LOG.info("Added catalog item with id {} to ES index {}", e.getId(), indexResponse.getIndex());
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    LOG.info("Updated catalog item with id {} to ES index {}, version of the object is {} ", e.getId(), indexResponse.getIndex(), indexResponse.getVersion());
                }

            } catch (IOException ex) {
                LOG.warn("Could not post {} to ES", e, ex);
            }
        });
    }

    public List<CatalogItem> findCatalogItem(String text) {
        try {
            //create a search request by passing an index
            // and then use a search query builder to construct a full text search
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SimpleQueryStringBuilder matchQueryBuilder = QueryBuilders.simpleQueryStringQuery(text);
            searchSourceBuilder.query(matchQueryBuilder);
            searchRequest.source(searchSourceBuilder);

            //The search response encapsulates the JSON navigation
            //and allows you easy access to the resulting documents via the SearchHits array
            SearchResponse response = getRestClient().search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            List<CatalogItem> catalogItems = Arrays.stream(searchHits)
                    .map(e -> mapJsonToCatalogItem(e.getSourceAsString()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            return catalogItems;
        } catch (IOException ex) {
            LOG.warn("Could not post {} to ES", text, ex);
        }
        return Collections.emptyList();
    }

    protected CatalogItem mapJsonToCatalogItem(String json) {
        try {
            return getObjectMapper().readValue(json, CatalogItem.class);
        } catch (IOException e) {
            LOG.warn("Could not convert {} to CatalogItem", json);
        }
        return null;
    }
}
