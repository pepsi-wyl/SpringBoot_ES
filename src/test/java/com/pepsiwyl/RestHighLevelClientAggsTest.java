package com.pepsiwyl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * @author by pepsi-wyl
 * @date 2022-08-04 17:15
 */

@Slf4j
public class RestHighLevelClientAggsTest extends SpringBootEsApplicationTests {

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelClientAggsTest(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    // 分组查询
    @Test
    @SneakyThrows
    public void testAggsGroup1() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 查询条件
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery()).size(0)
                .aggregation(AggregationBuilders.terms("price_group").field("price")));
        // 查询文档
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ParsedDoubleTerms parsedDoubleTerms = search.getAggregations().get("price_group");
        parsedDoubleTerms.getBuckets().forEach((entry -> {
            log.info("key: " + entry.getKey() + "   " + "value:" + entry.getDocCount());
        }));

        //关闭资源
        restHighLevelClient.close();
    }

    // 分组查询
    @Test
    @SneakyThrows
    public void testAggsGroup2() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 查询条件
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery()).size(0)
                .aggregation(AggregationBuilders.terms("title_group").field("title")));
        // 查询文档
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ParsedStringTerms parsedStringTerms = search.getAggregations().get("title_group");
        parsedStringTerms.getBuckets().forEach((entry -> {
            log.info("key: " + entry.getKey() + "   " + "value:" + entry.getDocCount());
        }));

        //关闭资源
        restHighLevelClient.close();
    }

    // 求最大值
    @Test
    @SneakyThrows
    public void testAggsMax() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 查询条件
        searchRequest.source(new SearchSourceBuilder()
                .aggregation(AggregationBuilders.max("price_max").field("price")));
        // 查询文档
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ParsedMax parsedMax = search.getAggregations().get("price_max");
        log.info(parsedMax.getValueAsString());

        //关闭资源
        restHighLevelClient.close();
    }

    // 求最小值
    @Test
    @SneakyThrows
    public void testAggsMin() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 查询条件
        searchRequest.source(new SearchSourceBuilder()
                .aggregation(AggregationBuilders.min("price_min").field("price")));
        // 查询文档
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ParsedMin parsedMin = search.getAggregations().get("price_min");
        log.info(parsedMin.getValueAsString());

        //关闭资源
        restHighLevelClient.close();
    }

    // 求平均值
    @Test
    @SneakyThrows
    public void testAggsAvg() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 查询条件
        searchRequest.source(new SearchSourceBuilder()
                .aggregation(AggregationBuilders.avg("price_avg").field("price")));
        // 查询文档
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ParsedAvg parsedAvg = search.getAggregations().get("price_avg");
        log.info(parsedAvg.getValueAsString());

        //关闭资源
        restHighLevelClient.close();
    }

    // 求和
    @Test
    @SneakyThrows
    public void testAggsSum() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("fruit");
        // 查询条件
        searchRequest.source(new SearchSourceBuilder()
                .aggregation(AggregationBuilders.sum("price_sum").field("price")));
        // 查询文档
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ParsedSum parsedSum = search.getAggregations().get("price_sum");
        log.info(parsedSum.getValueAsString());

        //关闭资源
        restHighLevelClient.close();
    }


}
