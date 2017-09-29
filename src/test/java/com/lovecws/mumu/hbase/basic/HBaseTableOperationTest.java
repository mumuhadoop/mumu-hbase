package com.lovecws.mumu.hbase.basic;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 表结构操作
 * @date 2017-09-27 10:45
 */
public class HBaseTableOperationTest {

    private static final Logger log = Logger.getLogger(HBaseTableOperationTest.class);
    private HBaseTableOperation hBaseTableOperation = new HBaseTableOperation();

    @Test
    public void createTable() {
        hBaseTableOperation.createTable("lovecws", "lover", "cws");
    }

    @Test
    public void disableTable() {
        hBaseTableOperation.disableTable("lovecws");
    }

    @Test
    public void enableTable() {
        hBaseTableOperation.enableTable("lovecws");
    }

    @Test
    public void listTables() {
        hBaseTableOperation.listTables();
    }

    @Test
    public void deleteTable() {
        hBaseTableOperation.deleteTable("babymm");
    }

    @Test
    public void addColumn() {
        hBaseTableOperation.addColumn("lovecws", "col");
    }
    
    @Test
    public void deleteColumn() {
        hBaseTableOperation.deleteColumn("lovecws", "col");
    }
}
