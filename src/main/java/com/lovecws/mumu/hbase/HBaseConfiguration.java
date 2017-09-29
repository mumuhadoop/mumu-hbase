package com.lovecws.mumu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase配置信息
 * @date 2017-09-27 9:26
 */
public class HBaseConfiguration {

    private static String HBASE_ZOOKEEPER_ADDRESS = "192.168.11.25";
    private static final Logger log = Logger.getLogger(HBaseConfiguration.class);

    private Connection connection;
    private Admin admin;
    private Table table;

    /**
     * 获取hbase连接
     *
     * @return
     */
    public Connection connection() {
        Configuration configuration = org.apache.hadoop.hbase.HBaseConfiguration.create();
        String hbase_zookeeper_address = System.getenv("HBASE_ZOOKEEPER_ADDRESS");
        if (hbase_zookeeper_address != null) {
            HBASE_ZOOKEEPER_ADDRESS = hbase_zookeeper_address;
        }
        configuration.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_ADDRESS);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            return connection;
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

    /**
     * 获取admin
     *
     * @return
     */
    public Admin admin() {
        if (connection == null) {
            connection();
        }
        try {
            admin = connection.getAdmin();
            return admin;
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

    /**
     * 创建htable实例
     *
     * @param tableName
     * @return
     */
    public Table table(String tableName) {
        try {
            if (admin == null) {
                admin();
            }
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                table = connection.getTable(tn);
            } else {
                log.info("表名【" + tableName + "】不存在");
            }
        } catch (IOException e) {
            log.error(e);
        }
        return table;
    }

    /**
     * 创建hbase连接池
     */
    public HTablePool pool() {
        Configuration configuration = org.apache.hadoop.hbase.HBaseConfiguration.create();
        String hbase_zookeeper_address = System.getenv("HBASE_ZOOKEEPER_ADDRESS");
        if (hbase_zookeeper_address != null) {
            HBASE_ZOOKEEPER_ADDRESS = hbase_zookeeper_address;
        }
        configuration.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_ADDRESS);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        HTablePool hTablePool = new HTablePool(configuration, 5);
        return hTablePool;
    }

    /**
     * 关闭资源
     */
    public void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
}
