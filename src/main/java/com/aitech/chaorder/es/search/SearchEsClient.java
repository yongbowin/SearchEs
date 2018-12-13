package com.aitech.chaorder.es.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author:
 * @Description:
 * @Date: Created in 17:54 2017/11/11 0011
 * @Modified By: Boson Wang
 */
public class SearchEsClient {
    private static final Logger esLogger = LogManager.getLogger(ImportJsonDataClient.class);
    private static String queryString = "新五丰";

    public static void main(String[] args) {
        /**
         * 开始时间
         */
        long startTime = System.currentTimeMillis();

        /**
         * 默认参数查询
         */
//        SearchEsCrud.defaultSearch();
        /**
         * 查询所有数据
         */
//        SearchEsCrud.matchAllImpl();
        /**
         * 条件查询
         */
//        SearchEsCrud.matchQueryImpl();
        /**
         * 完全匹配一个值
         */
//        SearchEsCrud.termQueryFun();
        /**
         * 一次匹配多个值 （***）
         */
//        SearchEsCrud.termsQueryFun();
        /**
         * 组合查询
         */
//        SearchEsCrud.combineQuery();
        /**
         * 高于设定的score查询，不计算相关性
         */
//        SearchEsCrud.constScoreQuery();
        /**
         * disMax查询 (****)
         */
//        SearchEsCrud.disMaxQueryImpl();
        /**
         * 模糊匹配，不能用通配符
         */
//        SearchEsCrud.fuzzyQueryImpl();
        /**
         * 实现基于内容推荐, 支持实现一句话相似文章查询 (****)
         */
//        SearchEsCrud.moreLikeThisQueryImple();
        /**
         * 范围内查询 (***)
         */
//        SearchEsCrud.rangeQueryImpl();
        /**
         * 通配符查询
         */
//        SearchEsCrud.wildCardQueryImpl();
        /**
         * hasChildQuery 查询
         */
//        SearchEsCrud.hasChildQueryImpl();
        /**
         * nested 嵌套查询 (****)
         */
        SearchEsCrud.nestedQueryImpl(queryString);

        /**
         * 结束时间, 设置日期格式，打印执行时间
         */
        long endTime = System.currentTimeMillis();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        esLogger.info(df.format(new Date()));// new Date()为获取当前系统时间
        float excTime = (float)(endTime - startTime) / 1000;
        esLogger.info("查询：“" + queryString + "”，执行时间：" + excTime + "s");
    }
}
