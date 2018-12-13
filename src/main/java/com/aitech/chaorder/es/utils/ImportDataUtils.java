package com.aitech.chaorder.es.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

/**
 * @author: Boson Wang
 * @Description: 从json文件中导入数据到es中, 导入查询数据, 使用的建立mapping的方式, 因为需要声明ik分词器
 * @Date: Created in 21:15 2017/11/8 0008
 * @Modified By: Boson Wang
 */
public class ImportDataUtils {
    private static final Logger esLogger = LogManager.getLogger(ImportDataUtils.class);
    private static final ResourceBundle SETTING = LoadConfig.getResourceBundle();
    private static final String ES_IMPORT_JSON_DATA = SETTING.getString("es.import.json.data");

    /**
     * 导入数据 以[]的json array数据
     *
     * @param index
     * @param type
     * @throws Exception
     */
    public static void importData(String index, String type, Client client) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File(ES_IMPORT_JSON_DATA)));

        StringBuilder sb = new StringBuilder();
        String line = null;
        while((line = br.readLine()) != null) {
            sb.append(line);
        }
        /**
         * 关闭bufferreader流，防止JVM内存溢出
         */
        br.close();
        BulkRequestBuilder prepareBulk = client.prepareBulk();
        JSONArray parseArray = JSON.parseArray(String.valueOf(sb));
        for (Object object : parseArray) {
            /**
             * 强转为map, 否则报错  the number of object passed must be even
             */
            Map<String, Object> source = (Map<String, Object>) object;
            /**
             * 嵌套子循环
             */
            JSONArray subjectSub = JSON.parseArray(String.valueOf(source.get("subject")));
            for (Object subjectSubSub : subjectSub) {
                Map<String, Object> subjectSubSubMap = (Map<String, Object>) subjectSubSub;
                /**
                 * 嵌套子循环
                 */
                JSONArray docReferencesSub = JSON.parseArray(String.valueOf(source.get("doc_references")));
                for (Object docReferencesSubSub : docReferencesSub) {
                    Map<String, Object> docReferencesSubSubMap = (Map<String, Object>) docReferencesSubSub;
                    XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                            .startObject()
                            .field("event_type", source.get("event_type"))
                            .field("process_start_time", source.get("process_start_time"))
                            .field("pipeline_version", source.get("pipeline_version"))
                            .startObject("subject")
                                .field("name", subjectSubSubMap.get("name"))
                                .field("entity", subjectSubSubMap.get("entity"))
                            .endObject()
                            .field("score", source.get("score"))
                            .field("event_group_tag", source.get("event_group_tag"))
                            .field("event_time", source.get("event_time"))
                            .field("tags", source.get("tags"))
                            .field("object", source.get("object"))
                            .field("modified_time", source.get("modified_time"))
                            .field("property", source.get("property"))
                            .field("created_time", source.get("created_time"))
                            .startObject("doc_references")
                                .field("disc_id", docReferencesSubSubMap.get("disc_id"))
                                .field("source", docReferencesSubSubMap.get("source"))
                                .field("title", docReferencesSubSubMap.get("title"))
                                .field("content", docReferencesSubSubMap.get("content"))
                            .endObject()
                            .endObject();
                    prepareBulk.add(client.prepareIndex(index, type).setSource(xContentBuilder));
                }
            }

//            esLogger.info("right =====> source.get==>subject: " + source.get("subject"));
        }
        BulkResponse response = prepareBulk.get();
    }

    /**
     * 创建mapping, 添加ik分词器等, 相当于创建数据库表
     * 索引库名: indices
     * 类型: mappingType
     * field("indexAnalyzer", "ik"): 字段分词ik索引
     * field("searchAnalyzer", "ik"): ik分词查询
     * @throws Exception
     */
    public static void createMapping(String indices, String type, Client client) throws Exception {
        /**
         * 创建index
         */
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 8);    // 分片数量
        settings.put("number_of_replicas", 0);    // 复制数量, 导入时最好为0, 之后2-3即可
        settings.put("refresh_interval", "10s");// 刷新时间

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate(indices);
        prepareCreate.setSettings(settings);

        /**
         * 创建mapping
         */
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
//                    .startObject("_ttl")//有了这个设置,就等于在这个给索引的记录增加了失效时间,
//                    //ttl的使用地方如在分布式下,web系统用户登录状态的维护.
//                        .field("enabled", true)//默认的false的
//                        .field("default", "5m")//默认的失效时间,d/h/m/s 即天/小时/分钟/秒
//                        .field("store", "yes")
//                        .field("index", "not_analyzed")
//                    .endObject()
//                     .startObject("_timestamp")//这个字段为时间戳字段.即你添加一条索引记录后,自动给该记录增加个时间字段(记录的创建时间),搜索中可以直接搜索该字段.
//                        .field("enabled", true)
//                        .field("store", "no")
//                        .field("index", "not_analyzed")
//                    .endObject()
                .startObject("properties")
//                .startObject("title").field("type", "text").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
                .startObject("event_type").field("type", "keyword").endObject()
                .startObject("process_start_time").field("type", "keyword").endObject()
                .startObject("pipeline_version").field("type", "keyword").endObject()
                .startObject("subject")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("name").field("type", "keyword").endObject()
                        .startObject("entity").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
                .startObject("score").field("type", "float").endObject()
                .startObject("event_group_tag").field("type", "keyword").endObject()
                .startObject("event_time").field("type", "keyword").endObject()
                .startObject("tags").field("type", "keyword").endObject()
                .startObject("object").field("type", "keyword").endObject()
                .startObject("modified_time").field("type", "keyword").endObject()
                .startObject("property").field("type", "keyword").endObject()
                .startObject("created_time").field("type", "keyword").endObject()
                .startObject("doc_references")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("disc_id").field("type", "keyword").endObject()
                        .startObject("source").field("type", "keyword").endObject()
                        .startObject("title").field("type", "text").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
                        .startObject("content").field("type", "text").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject();
        esLogger.info("ImportDataUtils.createMapping: mapping.string()=" + mapping.string());
        prepareCreate.addMapping(type, mapping);
        CreateIndexResponse response = prepareCreate.execute().actionGet();
        esLogger.info("ImportDataUtils.createMapping: response=" + response);
        mapping.close();
    }

}