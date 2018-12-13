package com.aitech.chaorder.es.search;

import com.aitech.chaorder.es.utils.ImportDataUtils;
import com.aitech.chaorder.es.utils.LoadConfig;
import com.aitech.chaorder.es.utils.TransportClientApi;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;

/**
 * @author: Boson Wang
 * @Description: search es CRUD
 * @Date: Created in 10:24 2017/11/8 0008
 * @Modified By: Boson Wang
 */
public class ImportJsonDataClient {
    private static final Logger esLogger = LogManager.getLogger(ImportJsonDataClient.class);
    private static final ResourceBundle SETTING = LoadConfig.getResourceBundle();
    private static final String ES_SEARCH_CLIENT_INDICES = SETTING.getString("es.search.client.indices");
    private static final String ES_SEARCH_CLIENT_INDEX = SETTING.getString("es.search.client.index");
    private static final String ES_SEARCH_CLIENT_TYPE = SETTING.getString("es.search.client.type");

    public static void main(String[] args) {
        /**
         * 开始时间
         */
        long startTime = System.currentTimeMillis();
        /**
         * 从json文件导入数据，index和type需要小写字母，并创建Mapping
         * 创建的先后顺序：先创建Mapping，再导入数据，如果直接导入数据就默认创建了Mapping
         */
        Client transPortClient = TransportClientApi.getTransPortClient();
        try {
            ImportDataUtils.createMapping(ES_SEARCH_CLIENT_INDICES, ES_SEARCH_CLIENT_TYPE, transPortClient);
        } catch(Exception e) {
            StringBuilder log=new StringBuilder();
            log.append("ImportJsonDataClient.main;");
            log.append("创建Mapping出错！");
            log.append(";ERR-MSG:");
            log.append(e.getMessage());
            log.append(",ERR-NAME：");
            log.append(e);
            esLogger.error(log.toString());
            esLogger.error(e.getLocalizedMessage(),e);
        }
        try {
            ImportDataUtils.importData(ES_SEARCH_CLIENT_INDEX, ES_SEARCH_CLIENT_TYPE, transPortClient);
        } catch(Exception e) {
            StringBuilder log=new StringBuilder();
            log.append("ImportJsonDataClient.main;");
            log.append("Json数据导入出错！");
            log.append(";ERR-MSG:");
            log.append(e.getMessage());
            log.append(",ERR-NAME：");
            log.append(e);
            esLogger.error(log.toString());
            esLogger.error(e.getLocalizedMessage(),e);
        }
        transPortClient.close();

//        EsCrud crud = new EsCrud(TransportClientApi.getTransPortClient());

//        long startTime = System.currentTimeMillis();
//        crud.createIndex(index);
//        crud.addMapping(index, type);
        //crud.deleteIndex(index);

//        crud.createDoc(index, type, "2");
        //crud.updateDoc(index, type, "2");
//        System.out.println(crud.getRecord(index, type, "3"));
        //System.out.println(crud.queryByFilter(index, type));
        //crud.deleteByQuery(index, type);
        //crud.min(index, type);

        /**
         * 结束时间, 设置日期格式，打印执行时间
         */
        long endTime = System.currentTimeMillis();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        esLogger.info(df.format(new Date()));// new Date()为获取当前系统时间
        float excTime = (float)(endTime - startTime) / 1000;
        esLogger.info("导入Json数据执行时间：" + excTime + "s");
    }
}
