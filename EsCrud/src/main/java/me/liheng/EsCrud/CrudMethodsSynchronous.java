package me.liheng.EsCrud;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.liheng.EsCrud.model.CatalogItem;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CrudMethodsSynchronous {
    static Logger LOG = LogManager.getLogger(CrudMethodsSynchronous.class);
    protected static final String HITS = "\"hits\":[";
    protected static final String SEARCH_FULL_TEXT = "{\r\n" +
            "    \"query\": {\r\n" +
            "        \"query_string\" : {\r\n" +
            "            \"query\" :  \"%s\"\r\n" +
            "        }\r\n" +
            "    }\r\n" +
            "}";

    private String index;
    private RestClient restClient;
    private ObjectMapper objectMapper;

    public CrudMethodsSynchronous(String index, RestClient restClient) {
        this.index = index;
        this.restClient = restClient;
        this.objectMapper = new ObjectMapper();
    }

    protected String getIndex() {
        return index;
    }

    protected RestClient getRestClient() {
        return restClient;
    }

    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }


    //http://localhost:9200/catalog_item_low_level/_doc/1
    public void createCatalogItem(List<CatalogItem> items) {

        items.stream().forEach(e-> {
            Request request = new Request("PUT", String.format("/%s/_doc/%d", getIndex(),  e.getId()));
            try {
                request.setJsonEntity(getObjectMapper().writeValueAsString(e));

                getRestClient().performRequest(request);
            } catch (IOException ex) {
                LOG.warn("Could not post {} to ES", e, ex);
            }
        });
    }

    public List<CatalogItem> findCatalogItem(String text) {
        Request request = new Request("GET", String.format("/%s/_search", getIndex()));
        request.setJsonEntity(String.format(SEARCH_FULL_TEXT, text));
        try {
            Response response = getRestClient().performRequest(request);
            if (response.getStatusLine().getStatusCode()==200) {
                List<CatalogItem> catalogItems = parseResultsFromFullSearch(response);
                return catalogItems;
            }
        } catch (IOException ex) {
            LOG.warn("Could not post {} to ES", text, ex);
        }
        return Collections.emptyList();
    }

    protected List<CatalogItem> parseResultsFromFullSearch(Response response)
            throws IOException, JsonParseException, JsonMappingException {
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.debug(responseBody);
        int startIndex = responseBody.indexOf(HITS);
        int endIndex = responseBody.indexOf("]}}");
        String json = responseBody.substring(startIndex, endIndex+1);
        json = "{" + json + "}";

        CatalogItemSearchResult sr = getObjectMapper().readValue(json, CatalogItemSearchResult.class);
        List<CatalogItem> catalogItems = sr.getHits().stream().map(e-> e.getSource()).collect(Collectors.toList());
        return catalogItems;
    }
}
