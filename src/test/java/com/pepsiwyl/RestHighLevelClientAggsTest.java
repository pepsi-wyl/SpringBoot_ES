package com.pepsiwyl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pepsiwyl.pojo.Product;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/**
 * @author by pepsi-wyl
 * @date 2022-08-04 17:15
 */

@Slf4j
public class RestHighLevelClientObjectTest extends SpringBootEsApplicationTests {

    private final RestHighLevelClient restHighLevelClient;

    @Autowired
    public RestHighLevelClientObjectTest(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @Test
    @SneakyThrows
    public void addObject() {

        // 准备对象
        Product product = new Product().setId(3).setTitle("小浣干吃面").setPrice(1.5).setDescription("小浣熊干吃面真好吃").setCreateTime(new Date());

        // 索引请求对象 索引名称
        IndexRequest indexRequest = new IndexRequest("products");
        // 添加文档的数据     id 手动指定文档的ID   source 指定文档的数据
        indexRequest.id(product.getId().toString()).source(new ObjectMapper().writeValueAsString(product), XContentType.JSON);

        // 添加文档       索引请求对象   请求配置对象(默认)
        IndexResponse save = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        log.info(save.toString());

        // 关闭资源
        restHighLevelClient.close();
    }

    @Test
    @SneakyThrows
    public void getObject() {

        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("products");
        // 指定查询条件
        searchRequest.source(new SearchSourceBuilder()
                .query(QueryBuilders.termQuery("description", "小浣熊"))
                .from(0).size(2)
                .sort("price", SortOrder.ASC)
                .highlighter(new HighlightBuilder().field("description").preTags("<span style='color:red;'>").postTags("</span>"))
        );

        // 查询文档         查询请求对象   请求配置对象(默认)
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        // 处理数据
        ArrayList<Product> productArrayList = new ArrayList<>();
        Arrays.asList(search.getHits().getHits()).forEach((v) -> {
            try {

                // JSON To Object
                Product product = new ObjectMapper().readValue(v.getSourceAsString(), Product.class);

                // 处理高亮
                if (v.getHighlightFields().containsKey("description")) {
                    Text description = v.getHighlightFields().get("description").fragments()[0];
                    product.setDescription(String.valueOf(description));
                }

                productArrayList.add(product);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        productArrayList.forEach(v -> log.info(v.toString()));

        // 关闭资源
        restHighLevelClient.close();
    }

}
