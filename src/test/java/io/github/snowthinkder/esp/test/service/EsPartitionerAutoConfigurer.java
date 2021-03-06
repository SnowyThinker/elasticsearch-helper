package io.github.snowthinkder.esp.test.service;

import org.elasticsearch.client.Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import io.github.snowthinker.eh.template.CustomDefaultResultMapper;

public class EsPartitionerAutoConfigurer {

	@Bean
	@Primary
	public ElasticsearchTemplate elasticsearchTemplate(Client client) {
		return new ElasticsearchTemplate(client, new CustomDefaultResultMapper());
	}
}
