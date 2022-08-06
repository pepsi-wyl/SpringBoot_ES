package com.pepsiwyl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;

/**
 * @author by pepsi-wyl
 * @date 2022-08-04 10:16
 */

@SuppressWarnings("all")

@Slf4j
public class RestHighLevelClientTest extends SpringBootEsApplicationTests {

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelClientTest(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    // 创建索引的时候创建映射
    @Test
    @SneakyThrows
    public void testCreateIndexAndMapping() {

        // 创建索引请求对象 索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("products");
        // 索引的配置    JSON串,XContentType.JSON
        createIndexRequest.settings("{\n" + "    \"number_of_shards\": 1,\n" + "    \"number_of_replicas\": 0\n" + "  }", XContentType.JSON);
        // 索引的映射    JSON串,XContentType.JSON
        createIndexRequest.mapping("{\n" + "    \"properties\": {\n" + "      \"title\":{\n" + "        \"type\": \"keyword\"\n" + "      },\n" + "      \"price\":{\n" + "        \"type\": \"double\"\n" + "      },\n" + "      \"description\":{\n" + "        \"type\": \"text\",\n" + "        \"analyzer\": \"ik_max_word\"\n" + "      },\n" + "      \"createTime\":{\n" + "        \"type\": \"date\"\n" + "      }\n" + "    }\n" + "  }", XContentType.JSON);

        // 创建索引        1.创建索引请求对象   2.请求配置对象(默认)
        CreateIndexResponse create = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.info(String.valueOf(create.isAcknowledged()));

        // 关闭资源
        restHighLevelClient.close();
    }

    // 删除索引
    @Test
    @SneakyThrows
    public void testDeleteIndex() {

        // 删除索引请求对象  索引名称
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("products");

        // 删除索引         删除索引请求对象   请求配置对象(默认)
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        log.info(String.valueOf(delete.isAcknowledged()));

        // 关闭资源
        restHighLevelClient.close();
    }

    // 添加文档
    @Test
    @SneakyThrows
    public void testSave() {

        // 索引请求对象 索引名称
        IndexRequest indexRequest = new IndexRequest("products");
        // 添加文档的数据     id 手动指定文档的ID   source 指定文档的数据
        indexRequest.id("1").source("{\n" + "  \"title\":\"小浣熊干吃面\",\n" + "  \"price\":1.5,\n" + "  \"description\":\"小浣熊干吃面真好吃\",\n" + "  \"createTime\":\"2022-12-12\"\n" + "}", XContentType.JSON);

        // 添加文档       索引请求对象   请求配置对象(默认)
        IndexResponse save = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        log.info(save.status().toString());

        // 关闭资源
        restHighLevelClient.close();
    }

    // 更新文档
    @Test
    @SneakyThrows
    public void testUpdated() {

        // 更新请求对象    索引名称  文档ID
        UpdateRequest updateRequest = new UpdateRequest("products", "1");
        // doc 指定更新的字段及数据
        updateRequest.doc("{\"title\":\"小浣熊\"}", XContentType.JSON);

        // 更新文档       更新请求对象   请求配置对象(默认)
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        log.info(update.status().toString());

        // 关闭资源
        restHighLevelClient.close();
    }

    // 删除文档
    @Test
    @SneakyThrows
    public void testDelete() {

        // 删除请求对象    索引名称  文档ID
        DeleteRequest deleteRequest = new DeleteRequest("products", "1");

        // 删除文档       更新请求对象   请求配置对象(默认)
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        log.info(delete.status().toString());

        // 关闭资源
        restHighLevelClient.close();
    }

    // 查询文档 根据ID
    @Test
    @SneakyThrows
    public void testQueryById() {

        // 查询请求对象    索引名称  文档ID
        GetRequest getRequest = new GetRequest("products", "1");

        // 查询文档 ByID   查询请求对象   请求配置对象(默认)
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        log.info("ID: " + getResponse.getId());
        log.info("source" + getResponse.getSourceAsString());
        log.info(getResponse.toString());

        // 关闭资源
        restHighLevelClient.close();
    }

    // 查询 参数为查询条件
    @SneakyThrows
    public void query(SearchSourceBuilder searchSourceBuilder) {

        // 搜索请求对象  索引名称
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定查询条件
        searchRequest.source(searchSourceBuilder);

        // 查询文档         查询请求对象   请求配置对象(默认)
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 总条数
        log.info("total: " + search.getHits().getTotalHits().value);
        // 最大得分
        log.info("maxScore: " + search.getHits().getMaxScore());
        // 获取结果
        Arrays.asList(search.getHits().getHits()).forEach((v) -> {
            log.info("ID: " + v.getId());
            log.info("source: " + v.getSourceAsString());
            v.getHighlightFields().forEach((key, value) -> System.out.println("key: " + key + " value: " + value.fragments()[0]));
        });
        log.info(search.toString());

        // 关闭资源
        restHighLevelClient.close();
    }


    @Test
    @SneakyThrows
    public void testSearch() {

        // 查询文档   所有
        query(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));

        // 查询文档   ids
//        query(new SearchSourceBuilder().query(QueryBuilders.idsQuery().addIds("1").addIds("2").addIds("3")));

        // 查询文档   关键词
//        query(new SearchSourceBuilder().query(QueryBuilders.termQuery("description", "浣熊")));

        // 查询文档   范围
//        query(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("price").gte(0).lte(100)));

        // 查询文档   前缀
//        query(new SearchSourceBuilder().query(QueryBuilders.prefixQuery("title", "小")));

        // 查询文档   通配符
//        query(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("title", "*干吃面*")));

        // 查询文档   模糊
//        query(new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("description", "小浣熊")));

        // 查询文档   布尔
//        query(new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("description", "浣熊"))));

        // 查询文档   多字段查询
//        query(new SearchSourceBuilder().query(QueryBuilders.multiMatchQuery("小浣熊", "title", "description")));

        // 查询文档   默认字段查询
//        query(new SearchSourceBuilder().query(QueryBuilders.queryStringQuery("小浣熊").field("description")));

        // 查询文档   分页查询   form起始位置 size页面大小默认为10  form=(page-1)*size
//        query(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).from(0).size(3));

        // 查询文档   排序 sort 默认ASC(升序)
//        query(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort("price", SortOrder.ASC));

        // 查询文档   返回指定的字段  1.包含字段数组  2.排除字段数组
//        query(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).fetchSource(new String[]{"title"}, new String[]{}));
//        query(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).fetchSource(new String[]{}, new String[]{"description"}));

        // 查询文档   高亮查询
//        HighlightBuilder highlightBuilder = new HighlightBuilder().requireFieldMatch(false).field("description").field("title").preTags("<span style='color:red;'>").postTags("</span>");
//        query(new SearchSourceBuilder().query(QueryBuilders.termQuery("description", "浣熊")).highlighter(highlightBuilder));

        // 查询文档   filter过滤    先执行filter后执行query 数据量大的时候效率高
//        query(new SearchSourceBuilder()
//                .query(QueryBuilders.matchAllQuery())
//                .postFilter(QueryBuilders.rangeQuery("price").gte(0).lte(100))
//        );
    }
}
