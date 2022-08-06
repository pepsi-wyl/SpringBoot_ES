package com.pepsiwyl.pojo;

import lombok.*;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;


/**
 * @author by pepsi-wyl
 * @date 2022-08-03 22:21
 */

@Builder
@Accessors(chain = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode

// 代表一个对象为一个文档  -- indexName属性: 创建索引的名称  -- createIndex属性: 索引不存在是否创建索引
@Document(indexName = "products", createIndex = true)
public class Product {

    // 将对象Id字段与ES中文档的_id对应
    @Id
    private Integer id;

    // 用来描述属性在ES中存储类型以及分词情况 -- type: 用来指定字段类型
    @Field(type = FieldType.Keyword)
    private String title;

    // 用来描述属性在ES中存储类型以及分词情况 -- type: 用来指定字段类型
    @Field(type = FieldType.Double)
    private Double price;

    // 用来描述属性在ES中存储类型以及分词情况 -- type: 用来指定字段类型 -- analyzer: 分词情况
    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String description;

    // 后来添的属性
    private Date createTime;

}