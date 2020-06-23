package me.liheng.EsCrud;

import me.liheng.EsCrud.model.CatalogItem;

import java.util.List;


public class CatalogItemSearchResult extends Hits<SearchResult<CatalogItem>> {

}

class Hits<T> {
    private List<T> hits;

    public List<T> getHits() {
        return hits;
    }

    public void setHits(List<T> hits) {
        this.hits = hits;
    }

}
