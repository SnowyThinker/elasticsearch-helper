package io.github.snowthinker.eh;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.elasticsearch.client.RestHighLevelClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

@Slf4j
public class IndexServiceImpl implements IndexService {

    //OkHttpClient okHttpClient;
    RestHighLevelClient restHighLevelClient;

    ObjectMapper objectMapper;

    private String url;

    private String username;

    private String password;

    private boolean authMode = false;

    //public static HttpHeaders httpHeaders;

    /*
    @PostConstruct
    public void init() throws UnsupportedEncodingException {
        String auth = String.format("%s:%s", username, password);
        String authHeader = String.format("Basic %s", Base64.encodeBase64String(auth.getBytes("UTF-8")));
        httpHeaders = new HttpHeaders();
        httpHeaders.set("Authorization", authHeader);
        httpHeaders.set("content-type", "application/json");
    }
     */

    public IndexServiceImpl() {
        this.restHighLevelClient = new OkHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    public IndexServiceImpl(String url, String username, String password) {
        this.objectMapper = new ObjectMapper();
        this.url = url;
        this.username = username;
        this.password = password;

        this.okHttpClient = new OkHttpClient.Builder()
                .authenticator(new Authenticator() {
                    @Nullable
                    @Override
                    public Request authenticate(@Nullable Route route, @NotNull Response response) throws IOException {
                        String credentials = Credentials.basic(username, password);
                        return response.request().newBuilder().header("Authorization", credentials).build();
                    }
                })
                .build();

        this.authMode = true;
    }

    public IndexServiceImpl(OkHttpClient okHttpClient, ObjectMapper objectMapper, String url, String username, String password) {
        this.okHttpClient = okHttpClient;
        this.objectMapper = objectMapper;
        this.url = url;
        this.username = username;
        this.password = password;

        this.authMode = true;
    }

    @Override
    public boolean existIndex(String indexName) {
        Request request = new Request.Builder()
                .url(url + "/" + indexName)
                .method("HEAD", null)
                .build();

        Response response = null;
        try {
            response = okHttpClient.newCall(request).execute();
        } catch (IOException e) {
            log.error("head index error", e.getMessage());
        }

        response.body().

        HttpEntity<Object> httpEntity = new HttpEntity<>(new HashMap<>(), httpHeaders);
        ResponseEntity<AcknowledgedResponse> responseEntity = null;
        try {
            responseEntity = restTemplate.exchange(url + "/" + indexName, HttpMethod.HEAD, httpEntity, AcknowledgedResponse.class);
        } catch(Exception e) {
            log.error("existIndex error: {}", e.getMessage());
        }

        if(null == responseEntity || responseEntity.getStatusCodeValue() != 200) {
            return false;
        }

        return true;
    }

    public boolean createIndex(String indexName, String indexTemplatePath) {

        boolean exists = existIndex(indexName);
        if(exists) {
            log.info("Index {} already exists.", indexName);
            return true;
        }

        String content = null;
        try {
            content = FileUtils.readFileToString(new File(indexTemplatePath), "UTF-8");
        } catch (IOException e) {
            log.error("Read file error: ", e);
        }

        String url = urls.split(",")[0];

        HttpEntity<String> httpEntity = new HttpEntity<>(content, httpHeaders);
        ResponseEntity<Map> responseEntity = null;
        try {
            responseEntity = restTemplate.exchange(url + "/" + indexName, HttpMethod.PUT, httpEntity, Map.class);
        } catch(Exception e) {
            log.error("createIndex error", e);
        }

        if(null == responseEntity || responseEntity.getStatusCodeValue() != 200) {
            return false;
        }

        return true;
    }

    @Override
    public List<String> aliasDetail(String alias) {
        String url = urls.split(",")[0];

        HttpEntity<String> httpEntity = new HttpEntity<>(httpHeaders);
        ResponseEntity<Map> responseEntity = null;

        try {
            responseEntity = restTemplate.exchange(url + "/" + alias, HttpMethod.GET, httpEntity, Map.class);
        } catch(Exception e) {
            log.error("aliasDetail error: {}", e.getMessage());
        }

        if(null == responseEntity || null == responseEntity.getBody()) {
            return new ArrayList<>();
        }

        return (List<String>) responseEntity.getBody().keySet().stream().collect(Collectors.toList());
    }

    @Override
    public boolean createAlias(String aliasName, List<String> fullIndexList, List<String> existIndexList, String removeIndex) {

        AliasActionDto aliasActionDto = new AliasActionDto();

        if(existIndexList.contains(removeIndex)) {
            AliasRemoveDto removeDto = AliasRemoveDto.builder().remove(new AliasOperationDto(removeIndex, aliasName)).build();
            aliasActionDto.getActions().add(removeDto);
        }

        List<AliasAddDto> aliasAddDtoList = fullIndexList.stream().map(index -> {
            return AliasAddDto.builder().add(new AliasOperationDto(index, aliasName)).build();
        }).collect(Collectors.toList());

        aliasActionDto.getActions().addAll(aliasAddDtoList);

        String url = urls.split(",")[0];

        HttpEntity httpEntity = new HttpEntity<Object>(aliasActionDto, httpHeaders);
        ResponseEntity<Map> responseEntity = null;
        try {
            responseEntity = restTemplate.exchange(url + "/_aliases", HttpMethod.POST, httpEntity, Map.class);
            String requestData = objectMapper.writeValueAsString(aliasActionDto);
            log.info("requestData: {}", requestData);
        } catch(Exception e) {
            log.error("createAlias error", e);
        }

        if(null == responseEntity || null == responseEntity.getBody()) {
            return false;
        }

        log.info("resp: {}", responseEntity);

        return true;
    }
}


