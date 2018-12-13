package com.aitech.chaorder.es.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.util.ResourceBundle;

/**
 * @author: Boson Wang
 * @Description: connect ES
 * @Date: Created in 14:27 2017/11/8 0008
 * @Modified By: Boson Wang
 */
public class TransportClientApi {
    private static final Logger esLogger = LogManager.getLogger(LoadConfig.class);
    private static final ResourceBundle SETTING = LoadConfig.getResourceBundle();
    private static final String ES_CLUSTER_NAME = SETTING.getString("es.cluster.name");
    private static TransportClient transPortClient = null;
    /**
     * 集群服务IP集合
     */
    private static final String ES_CLUSTER_SETS_IP = SETTING.getString("es.cluster.sets.ip");
    /**
     * ES集群端口
     */
    private static final Integer ES_CLUSTER_SETS_PORT = Integer.parseInt(SETTING.getString("es.cluster.sets.port"));

    public static TransportClient getTransPortClient() {
        try {
            if (transPortClient == null) {
                if(ES_CLUSTER_SETS_IP == null || "".equals(ES_CLUSTER_SETS_IP.trim())){
                    return  null;
                }
                Settings settings = Settings.builder()
                        .put("cluster.name", ES_CLUSTER_NAME)
                        /**
                         * 自动把集群下的机器添加到列表中
                         */
                        .put("client.transport.sniff", true)
                        .build();
                transPortClient  = new PreBuiltTransportClient(settings);
                String esIps[] = ES_CLUSTER_SETS_IP.split(",");
                /**
                 * 添加集群IP列表
                 */
                for (String esIp : esIps) {
                    TransportAddress transportAddress =  new InetSocketTransportAddress(InetAddresses.forString(esIp), ES_CLUSTER_SETS_PORT);
                    transPortClient.addTransportAddresses(transportAddress);
                }
                return transPortClient;
            } else {
                return transPortClient;
            }
        } catch (Exception e) {
            StringBuilder log=new StringBuilder();
            log.append("TransportClientApi.getTransPortClient;");
            log.append("transPortClient=");
            log.append(transPortClient);
            log.append(";ERR-MSG:");
            log.append(e.getMessage());
            log.append(",ERR-NAME：");
            log.append(e);
            esLogger.error(log.toString());
            esLogger.error(e.getLocalizedMessage(),e);
            if (transPortClient != null) {
                transPortClient.close();
            }
            return null;
        }
    }

}
