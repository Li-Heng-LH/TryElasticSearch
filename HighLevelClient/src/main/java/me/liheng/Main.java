package me.liheng;

import me.liheng.model.CatalogItem;
import me.liheng.model.CatalogItemUtil;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;

public class Main {

    static Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")))) {
            ClientCrudMethods scm = new ClientCrudMethods("catalog_item_high_level",  client);
//            scm.createCatalogItem(CatalogItemUtil.getCatalogItems());

            List<CatalogItem> items = scm.findCatalogItem("flashlight");
            LOG.info("Found {} items: {}", items.size(), items);
        }
    }
}
