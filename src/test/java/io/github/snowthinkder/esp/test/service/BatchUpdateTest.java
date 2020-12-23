package io.github.snowthinkder.esp.test.service;

import java.util.Collections;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchUpdateTest {
	
	@Autowired
	private Client client;

	public void batchUpdate() {
		
		QueryBuilder queryBuilder = QueryBuilders.termQuery("warehouseId", 1);
		
		UpdateByQueryRequestBuilder updateByQuery = new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
		updateByQuery
			.source("Item201811")
			.abortOnVersionConflict(true)
			.filter(queryBuilder)
			.script(new Script(ScriptType.INLINE, "painless", "ctx._source.deleted=1", Collections.emptyMap()));
		
		BulkByScrollResponse response = updateByQuery.get();
		
		if(response.getBatches() > 0) {
			log.info("Batch update success");
		} else {
			log.info("No record matched");
		}
		
	}
}
