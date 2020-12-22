package io.github.snowthinker.eh;

import java.util.List;

public interface IndexService {

    boolean existIndex(String indexName);

    boolean createIndex(String indexName, String indexTemplatePath);

    List<String> aliasDetail(String alias);

    boolean createAlias(String aliasName, List<String> fullIndexList, List<String> existIndexList, String removeIndex);
}

