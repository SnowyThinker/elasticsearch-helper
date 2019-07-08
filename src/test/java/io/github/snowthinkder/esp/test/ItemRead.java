package io.github.snowthinkder.esp.test;

import org.springframework.data.elasticsearch.annotations.Document;

@SuppressWarnings("serial")
@Document(indexName = "item_read", type="item")
public class ItemRead extends Item {

}
