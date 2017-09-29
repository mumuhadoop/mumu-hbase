package com.lovecws.mumu.hbase;

import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase配置信息测试
 * @date 2017-09-27 10:39
 */
public class HBaseConfigurationTest {

    private HBaseConfiguration configuration = new HBaseConfiguration();

    @Test
    public void connection() {
        Connection connection = configuration.connection();
        System.out.println(connection);
        configuration.close();
    }

    @Test
    public void admin() {
        Admin admin = configuration.admin();
        System.out.println(admin);
        configuration.close();
    }

    @Test
    public void table() {
        Table table = configuration.table("lovecws");
        System.out.println(table.getClass().getName());
        configuration.close();
    }

    @Test
    public void pool() {
        HTablePool hTablePool = configuration.pool();
        for (int i = 0; i < 10; i++) {
            HTableInterface table = hTablePool.getTable("lovecws");
            System.out.println(table + "" + table.hashCode());
        }
        try {
            hTablePool.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
