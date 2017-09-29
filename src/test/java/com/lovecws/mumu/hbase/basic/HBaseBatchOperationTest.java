package com.lovecws.mumu.hbase.basic;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 批量操作测试
 * @date 2017-09-27 14:47
 */
public class HBaseBatchOperationTest {

    private HBaseBatchOperation baseBatchOperation = new HBaseBatchOperation();

    @Test
    public void putBatch() {
        baseBatchOperation.putBatch("babymm", "row4", "baby", "b", "mm");
    }

    @Test
    public void getBatch() {
        baseBatchOperation.getBatch("babymm", "row34", "row35");
    }

    @Test
    public void deleteBatch() {
        baseBatchOperation.deleteBatch("babymm", "row499", "row498");
    }
}
