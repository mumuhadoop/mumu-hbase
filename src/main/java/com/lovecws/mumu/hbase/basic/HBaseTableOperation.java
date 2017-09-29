package com.lovecws.mumu.hbase.basic;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 创建hbase table
 * @date 2017-09-27 9:26
 */
public class HBaseTableOperation {

    private static final Logger log = Logger.getLogger(HBaseTableOperation.class);

    /**
     * 创建表
     *
     * @param tableName 表名称
     * @param columns   列族名称
     */
    public void createTable(String tableName, String... columns) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        try {
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                log.info("表名【" + tableName + "】已存在");
                return;
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
            for (String column : columns) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(column);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public void deleteTable(String tableName) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);//先禁用表 才能删除
                admin.deleteTable(tn);
            } else {
                log.info("表名【" + tableName + "】不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 激活表
     *
     * @param tableName
     */
    public void enableTable(String tableName) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                admin.enableTable(tn);
            } else {
                log.info("表名【" + tableName + "】不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 禁用表
     *
     * @param tableName
     */
    public void disableTable(String tableName) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
            } else {
                log.info("表名【" + tableName + "】不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 列出所有的表
     */
    public void listTables() {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        try {
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                log.info("表名:" + hTableDescriptor.getTableName());
                log.info("列族:" + JSON.toJSONString(hTableDescriptor.getColumnFamilies()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 往表中添加列族
     *
     * @param tableName  表名
     * @param familyName 列族名
     */
    public void addColumn(String tableName, String familyName) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        TableName tb = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tb)) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyName);

                columnDescriptor.setMaxVersions(1);//设置列族保留的最多版本
                columnDescriptor.setCompressionType(Compression.Algorithm.GZ);//设置压缩算法
                columnDescriptor.setCompactionCompressionType(Compression.Algorithm.GZ);//合并压缩算法

                admin.addColumn(tb, columnDescriptor);
            } else {
                log.info("表名【" + tableName + "】不存在");
            }
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 往表中删除列族
     *
     * @param tableName  表名
     * @param familyName 列族名
     */
    public void deleteColumn(String tableName, String familyName) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Admin admin = hBaseConfiguration.admin();
        TableName tb = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tb)) {
                admin.deleteColumn(tb, Bytes.toBytes(familyName));
            } else {
                log.info("表名【" + tableName + "】不存在");
            }

        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }
}
