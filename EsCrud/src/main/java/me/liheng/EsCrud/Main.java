package me.liheng.EsCrud;

import me.liheng.EsCrud.model.CatalogItem;
import me.liheng.EsCrud.model.CatalogItemUtil;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import java.util.List;

public class Main {
    static Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        //java try-with-resources
        //it is a try statement that declares one or more resources
        //a resource is an object that must be closed after the program is finished with it
        //try-with-resources statement ensures that each resource is closed at the end of the statement
        //resource will be closed regardless of whether the try statement completes normally or abruptly
        try (RestClient client = RestClient.builder(
                new HttpHost("localhost", 9200, "http")).build()) {

            CrudMethodsSynchronous scm = new CrudMethodsSynchronous("catalog_item_low_level", client);
//            scm.createCatalogItem(CatalogItemUtil.getCatalogItems());

            List<CatalogItem> items = scm.findCatalogItem("flashlight");
            LOG.info("Found {} items: {}", items.size(), items);
        }
    }
}
