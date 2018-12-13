package com.aitech.chaorder.es.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * @author:
 * @Description:
 * @Date: Created in 14:39 2017/11/8 0008
 * @Modified By: Boson Wang
 */
public class LoadConfig {
    private static final Logger esLogger = LogManager.getLogger(LoadConfig.class);
    private static final String configPath = System.getProperty("user.dir") + "/config/searchES.properties";
    private static BufferedInputStream bufferedInputStream;
    private static ResourceBundle resourceBundle;

    static {
        try {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(configPath));
            resourceBundle = new PropertyResourceBundle(bufferedInputStream);
            bufferedInputStream.close();
        } catch (FileNotFoundException e) {
            StringBuilder log=new StringBuilder();
            log.append("LoadConfig static block FileNotFoundException;");
            log.append("Couldn't find the property file;");
            log.append("configPath=");
            log.append(configPath);
            log.append(";ERR-MSG:");
            log.append(e.getMessage());
            log.append(",ERR-NAME：");
            log.append(e);
            esLogger.error(log.toString());
            esLogger.error(e.getLocalizedMessage(),e);
            System.exit(0);
        } catch (IOException e) {
            StringBuilder log=new StringBuilder();
            log.append("LoadConfig static block IOException;");
            log.append("Couldn't find the property file;");
            log.append("configPath=");
            log.append(configPath);
            log.append(";ERR-MSG:");
            log.append(e.getMessage());
            log.append(",ERR-NAME：");
            log.append(e);
            esLogger.error(log.toString());
            esLogger.error(e.getLocalizedMessage(),e);
            System.exit(0);
        } catch (Exception e) {
            StringBuilder log=new StringBuilder();
            log.append("LoadConfig static block Exception;");
            log.append("configPath=");
            log.append(configPath);
            log.append(";ERR-MSG:");
            log.append(e.getMessage());
            log.append(",ERR-NAME：");
            log.append(e);
            esLogger.error(log.toString());
            esLogger.error(e.getLocalizedMessage(),e);
            System.exit(0);
        }
    }

    public static ResourceBundle getResourceBundle() {
        return resourceBundle;
    }
}
