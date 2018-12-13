package com.aitech.chaorder.es.search;

import com.aitech.chaorder.es.utils.FormatJsonString;
import com.aitech.chaorder.es.utils.TransportClientApi;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * @author:
 * @Description:
 * @Date: Created in 19:07 2017/11/9 0009
 * @Modified By: Boson Wang
 */
public class SearchEsCrud {
    private static final Logger esLogger = LogManager.getLogger(ImportJsonDataClient.class);

    /* ****************************************Java查询ES API*********************************** */

    /**
     * @author: Boson Wang
     * @description: MatchAll on the whole cluster with all default options
     * @date: 2017/11/13 0013 20:53
     * @param null
     * @return:
     */
    public static void defaultSearch() {
        Client transPortClient = TransportClientApi.getTransPortClient();
        SearchResponse response = transPortClient.prepareSearch().get();
        esLogger.info("default search result: " + response);
    }

    /**
     * @author: Boson Wang
     * @description: 查询所有数据
     * @date: 2017/11/13 0013 21:06
     * @param null
     * @return:
     */
    public static void matchAllImpl() {
        QueryBuilder queryBuilder = matchAllQuery();
        searchFunction(queryBuilder);
    }

    /**
     * @Use: match query
     * @Description: name是field,后边的是要查询的字符串 单个匹配 matchQuery
     */
    public static void matchQueryImpl() {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("event_type", "股份回购公告");
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 完全匹配一个值 termQuery
     * @date: 2017/11/14 0014 10:05
     * @param null
     * @return:
     */
    public static void termQueryFun() {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("event_type", "股份回购公告");
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 一次匹配多个值
     * @date: 2017/11/14 0014 14:34
     * @param null
     * @return:
     */
    public static void termsQueryFun() {
        QueryBuilder queryBuilder = QueryBuilders.termsQuery("event_type", "股份", "回购", "公告");
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 组合查询，其中termQuery表示完全匹配，并不是模糊匹配
     * @date: 2017/11/14 0014 14:47
     * @param null
     * @return:
     */
    public static void combineQuery() {
        /**
         * must(QueryBuilders)   : AND
         * mustNot(QueryBuilders): NOT
         * should:               : OR
         */
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("event_group_tag", "辉丰股份 发布 股份回购公告"))
                .mustNot(QueryBuilders.termQuery("event_type", "股份回购"))
                .should(QueryBuilders.termQuery("score", "1.0"));
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 包裹查询, 高于设定的score查询，不计算相关性
     * @date: 2017/11/14 0014 15:15
     * @param null
     * @return:
     */
    public static void constScoreQuery() {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("event_type", "股份回购公告")).boost(2.0f);
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: disMax查询, 对子查询的结果做union, score沿用子查询score的最大值, 广泛用于muti-field查询
     * 参考：http://blog.csdn.net/dm_vincent/article/details/41820537
     * 使用dis_max查询(Disjuction Max Query)。Disjuction的意思"OR"(而Conjunction的意思是"AND")，因此Disjuction Max Query
     * 的意思就是返回匹配了任何查询的文档，并且分值是产生了最佳匹配的查询所对应的分值。
     * 注意：dis_max查询只是简单的使用最佳匹配查询子句得到的_score，通过tie_breaker参数，所有匹配的子句都会起作用，只不过最佳
     * 匹配子句的作用更大。tie_breaker参数一个合理的值会靠近0，(比如，0.1 -0.4)，来确保不会压倒dis_max查询具有的最佳匹配性质。
     * @date: 2017/11/14 0014 15:19
     * @param null
     * @return: 
     */
    public static void disMaxQueryImpl() {
        QueryBuilder queryBuilder = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("event_type", "回购"))
                .add(QueryBuilders.termQuery("event_group_tag", "回购"))
                .boost(1.0f)
                .tieBreaker(0.3f);
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 模糊匹配，不能用通配符
     * @date: 2017/11/14 0014 15:46
     * @param null
     * @return:
     */
    public static void fuzzyQueryImpl() {
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("event_type", "股份回购公告");
//        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("doc_references.title", "三环集团");
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 实现基于内容推荐, 支持实现一句话相似文章查询
     * @date: 2017/11/14 0014 15:57
     * @param null
     * @return: 
     */
    public static void moreLikeThisQueryImple() {
        /**
         * minTermFreq 最少出现的次数
         * maxQueryTerms 最多允许查询的词语
         */
//        QueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery()
//                .minTermFreq(1)
//                .maxQueryTerms(12);
//        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 范围内查询
     * @date: 2017/11/14 0014 16:21
     * @param null
     * @return:
     */
    public static void rangeQueryImpl() {
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("event_type")
                .from("股")
                .to("公")
                .includeLower(true)     // 包含上界
                .includeUpper(true);      // 包含下届
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 通配符查询, 支持 * 匹配任何字符序列, 包括空。避免* 开始, 会检索大量内容造成效率缓慢
     * @date: 2017/11/14 0014 16:45
     * @param null
     * @return: 
     */
    public static void wildCardQueryImpl() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("event_type", "股*告");
//        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("doc_references.title", "三环集团*");
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description: 嵌套查询, 内嵌文档查询
     * @date: 2017/11/14 0014 16:54
     * @param null
     * @return:
     */
    public static void nestedQueryImpl(String queryString) {
        QueryBuilder queryBuilder = QueryBuilders.nestedQuery(
                "doc_references",
                QueryBuilders.boolQuery()
//                        .must(QueryBuilders.matchQuery("doc_references.title", queryString)),
                        .must(QueryBuilders.termsQuery("doc_references.title", queryString)),
                ScoreMode.Total
        );
        esLogger.info("查询请求：" + queryString);
        searchFunction(queryBuilder);
    }

    /**
     * @author: Boson Wang
     * @description:
     * @date: 2017/11/14 0014 18:51
     * @param null
     * @return: 
     */
    public static void hasChildQueryImpl() {
//        HasChildQueryBuilder queryBuilder = QueryBuilders.hasChildQuery("sonDoc", QueryBuilders.termQuery("name", "vini"));
//        searchFunction(queryBuilder);
    }




    /* ****************************************查询参数设置************************************* */
    /**
     * @author: Boson Wang
     * @description: 对es进行查询
     * @date: 2017/11/14 0014 10:13
     * @param null
     * @return:
     */
    private static void searchFunction(QueryBuilder queryBuilder) {
        /**
         * 创建连接，连接集群
         */
        Client transPortClient = TransportClientApi.getTransPortClient();
        /**
         * 定义需要返回的字段，和不需要返回的字段
         */
        String [] includeFields = new String[] {
                "event_time",
                "doc_references.title",
                "doc_references.disc_id",
                "doc_references.source",
                "event_type"
        };
//        String [] excludeFields = new String[] {"doc_references.content"};

        SearchResponse response = transPortClient.prepareSearch("demo_one")
                .setTypes("demo_instance").setFetchSource(includeFields, null)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(new TimeValue(60000))
                .setQuery(queryBuilder)
//                .setFrom(0).setExplain(true)
                .setSize(100).execute().actionGet();

        esLogger.info("命中数：" + response.getHits().totalHits + ", 查询结果：" + FormatJsonString.format(response.toString()));

        while(true) {
            esLogger.info("开始执行true循环");
            response = transPortClient.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            for (SearchHit hit : response.getHits()) {
                Iterator<Map.Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
                while(iterator.hasNext()) {
                    Map.Entry<String, Object> next = iterator.next();
                    esLogger.info("一共命中 " + response.getHits().totalHits + " 个记录 ");
                    esLogger.info(next.getKey() + ": " + next.getValue());
                    if(response.getHits().totalHits == 0) {
                        break;
                    }
                }
            }
            break;
        }
//        esLogger.info("分页后：" + response.getHits().totalHits + ", 查询结果：" + response);
        testResponse(response);
    }

    /**
     * @author: Boson Wang
     * @description: 对response结果的分析（测试来用）
     * @date: 2017/11/14 0014 10:15
     * @param null
     * @return:
     */
    public static void testResponse(SearchResponse response) {
        // 命中的记录数
        long totalhits = response.getHits().totalHits;

//        for (SearchHit searchHit : response.getHits()) {
//            // 打分
//            float score = searchHit.getScore();
//            // 文章id
//            int id = Integer.parseInt(searchHit.getSource().get("id").toString());
//            // title
//            String title = searchHit.getSource().get("title").toString();
//            // 内容
//            String content = searchHit.getSource().get("content").toString();
//            // 文章更新时间
//            long updatetime = Long.parseLong(searchHit.getSource().get("updatetime").toString());
//        }
    }

}
