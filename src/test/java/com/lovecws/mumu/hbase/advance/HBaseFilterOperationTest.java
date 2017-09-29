package com.lovecws.mumu.hbase.advance;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase过滤器测试
 * @date 2017-09-27 17:14
 */
public class HBaseFilterOperationTest {

    private HBaseFilterOperation baseFilterOperation = new HBaseFilterOperation();

    @Test
    public void rowFilter() {
        baseFilterOperation.rowFilter("babymm", "row1", 10);
    }

    @Test
    public void familyFilter() {
        baseFilterOperation.familyFilter("babymm", "baby", 10);
    }

    @Test
    public void qualifierFilter() {
        baseFilterOperation.qualifierFilter("babymm", "m1", 10);
    }

    @Test
    public void valueFilter() {
        baseFilterOperation.valueFilter("babymm", "mm1", 10);
    }

    @Test
    public void dependentColumnFilter() {
        baseFilterOperation.dependentColumnFilter("babymm", "mm", "m1", "mm1", 10);
    }

    @Test
    public void singleColumnValueFilter() {
        baseFilterOperation.singleColumnValueFilter("babymm", "mm", "m1", "mm1", 10);
    }

    @Test
    public void SingleColumnValueExcludeFilter() {
        baseFilterOperation.SingleColumnValueExcludeFilter("babymm", "mm", "m1", "mm1", 10);
    }

    @Test
    public void prefixFilter() {
        baseFilterOperation.prefixFilter("babymm", "row", 10);
    }

    @Test
    public void pageFilter() {
        baseFilterOperation.pageFilter("babymm", 1);
        baseFilterOperation.pageFilter("babymm", 1);
    }

    @Test
    public void keyOnlyFilter() {
        baseFilterOperation.keyOnlyFilter("babymm", 10);
    }

    @Test
    public void inclusiveStopFilter() {
        baseFilterOperation.inclusiveStopFilter("babymm", "row1", 10);
    }

    @Test
    public void timestampsFilter() {
        List<Long> list = new ArrayList<Long>();
        list.add(1506496994033l);
        baseFilterOperation.timestampsFilter("babymm", list, 10);
    }

    @Test
    public void columnCountGetFilter() {
        baseFilterOperation.columnCountGetFilter("babymm", 2, 10);
    }

    @Test
    public void columnPaginationFilter() {
        baseFilterOperation.columnPaginationFilter("babymm", 2, 2, 10);
    }

    @Test
    public void columnPrefixFilter() {
        baseFilterOperation.columnPrefixFilter("babymm", "b", 10);
    }

    @Test
    public void randomRowFilter() {
        baseFilterOperation.randomRowFilter("babymm", 0.4f, 10);
    }

    @Test
    public void skipFilter() {
        baseFilterOperation.skipFilter("babymm", "mm5", 10);
    }

    @Test
    public void whileMatchFilter() {
        baseFilterOperation.skipFilter("babymm", "row1", 10);
    }

    @Test
    public void filterList() {
        baseFilterOperation.filterList("babymm", "row", "baby", "b1.*", 10);
    }
}
