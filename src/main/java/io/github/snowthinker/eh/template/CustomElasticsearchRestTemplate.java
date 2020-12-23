package io.github.snowthinker.eh.template;

import static org.springframework.util.CollectionUtils.isEmpty;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.core.DefaultResultMapper;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.EsClient;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.ResultsMapper;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.facet.FacetRequest;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.query.IndexBoost;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.ScriptField;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.elasticsearch.core.query.SourceFilter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * 
 * @author Andrew
 *
 */
public class CustomElasticsearchRestTemplate extends ElasticsearchRestTemplate 
	implements ElasticsearchOperations, EsClient<RestHighLevelClient>, ApplicationContextAware{
	
	/*private static final Logger QUERY_LOGGER = LoggerFactory.getLogger("org.springframework.data.elasticsearch.core.QUERY");
	
	private static final String FIELD_SCORE = "_score";*/
	
	private RestHighLevelClient client;
	
//	@SuppressWarnings("unused")
	private ElasticsearchConverter elasticsearchConverter;
	
	private ResultsMapper resultsMapper;
	
//	private String searchTimeout;
	
	public CustomElasticsearchRestTemplate(RestHighLevelClient client) {
		super(client);
		
		MappingElasticsearchConverter mappingElasticsearchConverter = createElasticsearchConverter();
		initialize(client, mappingElasticsearchConverter,
				new DefaultResultMapper(mappingElasticsearchConverter.getMappingContext()));
	}

	public CustomElasticsearchRestTemplate(RestHighLevelClient client, ElasticsearchConverter elasticsearchConverter,
			EntityMapper entityMapper) {
		super(client);
		initialize(client, elasticsearchConverter,
				new DefaultResultMapper(elasticsearchConverter.getMappingContext(), entityMapper));
	}

	public CustomElasticsearchRestTemplate(RestHighLevelClient client, ResultsMapper resultsMapper) {
		super(client);
		initialize(client, createElasticsearchConverter(), resultsMapper);
	}

	public CustomElasticsearchRestTemplate(RestHighLevelClient client, ElasticsearchConverter elasticsearchConverter) {
		super(client);
		initialize(client, elasticsearchConverter, new DefaultResultMapper(elasticsearchConverter.getMappingContext()));
	}

	public CustomElasticsearchRestTemplate(RestHighLevelClient client, ElasticsearchConverter elasticsearchConverter,
			ResultsMapper resultsMapper) {
		super(client);
		initialize(client, elasticsearchConverter, resultsMapper);
	}

	private void initialize(RestHighLevelClient client, ElasticsearchConverter elasticsearchConverter,
			ResultsMapper resultsMapper) {

		Assert.notNull(client, "Client must not be null!");
		Assert.notNull(elasticsearchConverter, "elasticsearchConverter must not be null.");
		Assert.notNull(resultsMapper, "ResultsMapper must not be null!");

		this.client = client;
		this.elasticsearchConverter = elasticsearchConverter;
		this.resultsMapper = resultsMapper;
	}

	private MappingElasticsearchConverter createElasticsearchConverter() {
		return new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext());
	}
	
	
	
	private void prepareSort(Query query, SearchSourceBuilder sourceBuilder,
			@Nullable ElasticsearchPersistentEntity<?> entity) {

		for (Sort.Order order : query.getSort()) {
			SortOrder sortOrder = order.getDirection().isDescending() ? SortOrder.DESC : SortOrder.ASC;

			if (ScoreSortBuilder.NAME.equals(order.getProperty())) {
				ScoreSortBuilder sort = SortBuilders //
						.scoreSort() //
						.order(sortOrder);

				sourceBuilder.sort(sort);
			} else {
				ElasticsearchPersistentProperty property = (entity != null) //
						? entity.getPersistentProperty(order.getProperty()) //
						: null;
				String fieldName = property != null ? property.getFieldName() : order.getProperty();

				FieldSortBuilder sort = SortBuilders //
						.fieldSort(fieldName) //
						.order(sortOrder);

				if (order.getNullHandling() == Sort.NullHandling.NULLS_FIRST) {
					sort.missing("_first");
				} else if (order.getNullHandling() == Sort.NullHandling.NULLS_LAST) {
					sort.missing("_last");
				}

				sourceBuilder.sort(sort);
			}
		}
	}

	
	
	@Nullable
	private ElasticsearchPersistentEntity<?> getPersistentEntity(@Nullable Class<?> clazz) {
		return clazz != null ? elasticsearchConverter.getMappingContext().getPersistentEntity(clazz) : null;
	}
	
	/**
	 * <p>Search after
	 * @param query	查询条件
	 * @param sorts 排序为必传条件
	 * @param searchAfters	第一页可为空，剩余页不能为空
	 * @param pageSize	分页大小
	 * @param clazz		要转换的结果类
	 * @return Page	分页结果
	 */
	/*public <T> AggregatedPage<T> searchAfter(SearchQuery query, List<SortBuilder<?>> sorts, 
			Object[] searchAfters, Integer pageSize, Class<T> clazz) {
		//SearchRequestBuilder requestBuilder = prepareSearch(query, clazz);
		SearchRequest searchRequest = prepareSearch(query, clazz);
//		searchRequest.
		SearchAfterBuilder searchAfterBuilder = new SearchAfterBuilder();
		searchAfterBuilder.setSortValues(searchAfters);
		
		SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder();
		
		if(null != searchAfters && searchAfters.length > 0) {
			searchRequest.searchAfter(searchAfters);	
		}
		
		if(null != sorts && sorts.size() > 0) {
			sorts.forEach(sort -> {
				requestBuilder.addSort(sort);
			});
		}
		
		requestBuilder.setSize(pageSize);
		
		SearchResponse response = doSearch(requestBuilder, query);
		return resultsMapper.mapResults(response, clazz, null);
	}*/
	
	/**
	 * <p>search after with aggregation
	 * @param query	查询条件
	 * @param sorts	排序
	 * @param searchAfters	search after 数组
	 * @param pageSize	分页大小
	 * @param resultsExtractor	结果抽取器
	 * @return T
	 */
	/*@SuppressWarnings("rawtypes")
	public <T> T searchAfterAggregation(SearchQuery query, List<SortBuilder> sorts, Object[] searchAfters, 
			Integer pageSize, ResultsExtractor<T> resultsExtractor) {
		SearchRequestBuilder requestBuilder = prepareSearch(query, resultsExtractor.getClass(), searchAfters);
		
		if(null != searchAfters && searchAfters.length > 0) {
			requestBuilder.searchAfter(searchAfters);	
		}
		
		if(null != sorts && sorts.size() > 0) {
			sorts.forEach(sort -> {
				requestBuilder.addSort(sort);
			});
		}
		
		requestBuilder.setSize(pageSize);
		
		SearchResponse response = doSearch(requestBuilder, query);
		return resultsExtractor.extract(response);
	}*/
	
	@SuppressWarnings("rawtypes")
	private String[] retrieveIndexNameFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[] { getPersistentEntityFor(clazz).getIndexName() };
		}
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	private String[] retrieveTypeFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[] { getPersistentEntityFor(clazz).getIndexType() };
		}
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	private void setPersistentEntityIndexAndType(Query query, Class clazz) {
		if (query.getIndices().isEmpty()) {
			query.addIndices(retrieveIndexNameFromPersistentEntity(clazz));
		}
		if (query.getTypes().isEmpty()) {
			query.addTypes(retrieveTypeFromPersistentEntity(clazz));
		}
	}
	
	private static String[] toArray(List<String> values) {
		String[] valuesAsArray = new String[values.size()];
		return values.toArray(valuesAsArray);
	}
	
	private <T> SearchRequest prepareSearch(Query query, Class<T> clazz, Object[] searchAfters) {
		setPersistentEntityIndexAndType(query, clazz);
		return prepareSearch(query, Optional.empty(), clazz, searchAfters);
	}

	private <T> SearchRequest prepareSearch(SearchQuery query, Class<T> clazz, Object[] searchAfters) {
		setPersistentEntityIndexAndType(query, clazz);
		return prepareSearch(query, Optional.ofNullable(query.getQuery()), clazz, searchAfters);
	}
	
	private SearchRequest prepareSearch(Query query, Optional<QueryBuilder> builder, 
			@Nullable Class<?> clazz, Object[] searchAfters) {
		Assert.notNull(query.getIndices(), "No index defined for Query");
		Assert.notNull(query.getTypes(), "No type defined for Query");

		int startRecord = 0;
		SearchRequest request = new SearchRequest(toArray(query.getIndices()));
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		request.types(toArray(query.getTypes()));
		sourceBuilder.version(true);
		sourceBuilder.trackScores(query.getTrackScores());
		
		//添加search_after支持
		if(null != searchAfters && searchAfters.length != 0) {
			sourceBuilder.searchAfter(searchAfters);	
		}

		if (builder.isPresent()) {
			sourceBuilder.query(builder.get());
		}

		if (query.getSourceFilter() != null) {
			SourceFilter sourceFilter = query.getSourceFilter();
			sourceBuilder.fetchSource(sourceFilter.getIncludes(), sourceFilter.getExcludes());
		}

		if (query.getPageable().isPaged()) {
			startRecord = query.getPageable().getPageNumber() * query.getPageable().getPageSize();
			sourceBuilder.size(query.getPageable().getPageSize());
		}
		sourceBuilder.from(startRecord);

		if (!query.getFields().isEmpty()) {
			sourceBuilder.fetchSource(toArray(query.getFields()), null);
		}

		if (query.getIndicesOptions() != null) {
			request.indicesOptions(query.getIndicesOptions());
		}

		if (query.getSort() != null) {
			prepareSort(query, sourceBuilder, getPersistentEntity(clazz));
		}

		if (query.getMinScore() > 0) {
			sourceBuilder.minScore(query.getMinScore());
		}

		if (query.getPreference() != null) {
			request.preference(query.getPreference());
		}

		if (query.getSearchType() != null) {
			request.searchType(query.getSearchType());
		}
		

		request.source(sourceBuilder);
		return request;
	}
	
	/*private SearchResponse doSearch(SearchRequest searchRequest, SearchQuery searchQuery, Object[] searchAfters) {
		Optional.ofNullable(searchQuery.getQuery());
		//searchRequest = prepareSearch(searchRequest, Optional.ofNullable(searchQuery.getQuery()), SearchResponse.class, searchAfters);

		searchRequest = this.pare
		try {
			return client.search(searchRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new ElasticsearchException("Error for search request with scroll: " + searchRequest.toString(), e);
		}
	}*/
	
	private SearchRequest prepareSearch(SearchRequest searchRequest, SearchQuery searchQuery) {
		if (searchQuery.getFilter() != null) {
			searchRequest.source().postFilter(searchQuery.getFilter());
		}

		if (!isEmpty(searchQuery.getElasticsearchSorts())) {
			for (SortBuilder sort : searchQuery.getElasticsearchSorts()) {
				searchRequest.source().sort(sort);
			}
		}

		if (!searchQuery.getScriptFields().isEmpty()) {
			// _source should be return all the time
			// searchRequest.addStoredField("_source");
			for (ScriptField scriptedField : searchQuery.getScriptFields()) {
				searchRequest.source().scriptField(scriptedField.fieldName(), scriptedField.script());
			}
		}

		if (searchQuery.getCollapseBuilder() != null) {
			searchRequest.source().collapse(searchQuery.getCollapseBuilder());
		}

		if (searchQuery.getHighlightFields() != null || searchQuery.getHighlightBuilder() != null) {
			HighlightBuilder highlightBuilder = searchQuery.getHighlightBuilder();
			if (highlightBuilder == null) {
				highlightBuilder = new HighlightBuilder();
			}
			if (searchQuery.getHighlightFields() != null) {
				for (HighlightBuilder.Field highlightField : searchQuery.getHighlightFields()) {
					highlightBuilder.field(highlightField);
				}
			}
			searchRequest.source().highlighter(highlightBuilder);
		}

		if (!isEmpty(searchQuery.getIndicesBoost())) {
			for (IndexBoost indexBoost : searchQuery.getIndicesBoost()) {
				searchRequest.source().indexBoost(indexBoost.getIndexName(), indexBoost.getBoost());
			}
		}

		if (!isEmpty(searchQuery.getAggregations())) {
			for (AbstractAggregationBuilder aggregationBuilder : searchQuery.getAggregations()) {
				searchRequest.source().aggregation(aggregationBuilder);
			}
		}

		if (!isEmpty(searchQuery.getFacets())) {
			for (FacetRequest aggregatedFacet : searchQuery.getFacets()) {
				searchRequest.source().aggregation(aggregatedFacet.getFacet());
			}
		}
		return searchRequest;
	}
	
	private SearchResponse doSearch(SearchRequest searchRequest, SearchQuery searchQuery, Object[] searchAfters) {
		prepareSearch(searchRequest, searchQuery);

		try {
			return client.search(searchRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new ElasticsearchException("Error for search request with scroll: " + searchRequest.toString(), e);
		}
	}
	
	/**
	 * <p> search_after query
	 * @param query The query conditions
	 * @param clazz The returned result class
	 * @param searchAfters The search_after conditions
	 * @param pageSize	The page size
	 * @return
	 */
	public <T> AggregatedPage<T> searchAfter(SearchQuery query, Class<T> clazz, 
			Object[] searchAfters, Integer pageSize) {
		return queryForPage(query, clazz, resultsMapper, searchAfters, pageSize);
	}
	
	/**
	 * <p> search_after aggregation query
	 * @param query The query conditions
	 * @param sorts The query sorts
	 * @param searchAfters The search_after conditions
	 * @param pageSize The page size
	 * @param resultsExtractor
	 * @return
	 */
	public <T> T searchAfterAggregation(SearchQuery query, List<SortBuilder> sorts, Object[] searchAfters, 
			Integer pageSize, ResultsExtractor<T> resultsExtractor) {
		//FIXME 
		/**
		 * SearchRequestBuilder requestBuilder = prepareSearch(query);
		
		if(null != searchAfters && searchAfters.length > 0) {
			requestBuilder.searchAfter(searchAfters);	
		}
		
		if(null != sorts && sorts.size() > 0) {
			sorts.forEach(sort -> {
				requestBuilder.addSort(sort);
			});
		}
		
		requestBuilder.setSize(pageSize);
		
		SearchResponse response = doSearch(requestBuilder, query);
		return resultsExtractor.extract(response);
		 */
		return null;
	}
	
	public <T> AggregatedPage<T> queryForPage(SearchQuery query, Class<T> clazz, SearchResultMapper mapper,
			Object[] searchAfters, Integer pageSize) {
		SearchResponse response = doSearch(prepareSearch(query, clazz, searchAfters), query, searchAfters);
		return mapper.mapResults(response, clazz, query.getPageable());
	}
}
