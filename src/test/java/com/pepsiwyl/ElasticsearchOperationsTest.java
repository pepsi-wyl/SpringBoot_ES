package com.pepsiwyl;

import com.pepsiwyl.pojo.Product;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.Query;

import java.util.Objects;

/**
 * @author by pepsi-wyl
 * @date 2022-08-03 22:28
 */

@Slf4j
public class ElasticsearchOperationsTest extends SpringBootEsApplicationTests {

    private final ElasticsearchOperations elasticsearchOperations;

    @Autowired
    public ElasticsearchOperationsTest(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    // 添加文档  save ID不存在 进行添加
    @Test
    public void testSave() {
        elasticsearchOperations.save(new Product().setId(1).setTitle("小浣熊干吃面").setPrice(1.5).setDescription("小浣熊干吃面真好吃"));
    }

    // 删除文档
    @Test
    public void testDelete() {
        log.info(elasticsearchOperations.delete(new Product().setId(1)));
    }

    // 删除所有文档
    @Test
    public void testDeleteAll() {
        log.info(elasticsearchOperations.delete(Query.findAll(), Product.class).toString());
    }

    // 查询文档
    @Test
    public void testSearch() {
        log.info(Objects.requireNonNull(elasticsearchOperations.get("1", Product.class)).toString());
    }

    // 查询所有文档
    @Test
    public void testSearchAll() {
        elasticsearchOperations.search(Query.findAll(), Product.class).forEach(productSearchHit -> log.info(productSearchHit.toString()));
    }

    // 修改文档  save ID存在 进行更新
    @Test
    public void testUpdated() {
        elasticsearchOperations.save(new Product().setId(1).setTitle("小浣熊干吃面").setPrice(1.5).setDescription("小浣熊干吃面真的的的好吃"));
    }

}