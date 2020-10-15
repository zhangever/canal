package com.alibaba.otter.canal.parse.inbound.mysql;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;

@Ignore
public class XABinlogDumpTest {

    @Test
    public void testSimple() {
        final MysqlEventParser controller = new MysqlEventParser();
        final EntryPosition startPosition = new EntryPosition("mysql-bin.000003", 123L);

        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress("127.0.0.1", 3306), "root", "root"));
        controller.setConnectionCharset(Charset.forName("UTF-8"));
        controller.setMasterPosition(startPosition);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<Entry>>() {

            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                        throws CanalSinkException,
                                                                                                        InterruptedException {

                for (Entry entry : entrys) {
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                        || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        continue;
                    }

                    if (entry.getEntryType() == EntryType.ROWDATA) {
                        RowChange rowChage = null;
                        try {
                            rowChage = RowChange.parseFrom(entry.getStoreValue());
                        } catch (Exception e) {
                            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:"
                                                       + entry.toString(), e);
                        }

                        EventType eventType = rowChage.getEventType();
                        System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getSchemaName(),
                            entry.getHeader().getTableName(),
                            eventType));

                        printXAInfo(rowChage.getPropsList());

                        for (RowData rowData : rowChage.getRowDatasList()) {
                            if (eventType == EventType.DELETE) {
                                print(rowData.getBeforeColumnsList());
                            } else if (eventType == EventType.INSERT) {
                                print(rowData.getAfterColumnsList());
                            } else {
                                System.out.println("-------> before");
                                print(rowData.getBeforeColumnsList());
                                System.out.println("-------> after");
                                print(rowData.getAfterColumnsList());
                            }
                        }
                    }
                }

                return true;
            }

        });
        controller.setLogPositionManager(new AbstractLogPositionManager() {

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }

            @Override
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
                System.out.println(logPosition);
            }
        });

        controller.start();

        try {
            Thread.sleep(100 * 1000L);
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
        controller.stop();
    }

    private void print(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private void printXAInfo(List<CanalEntry.Pair> pairs) {
        if (pairs == null) {
            return;
        }

        String xaType = null;
        String xaXid = null;
        for (CanalEntry.Pair pair : pairs) {
            String key = pair.getKey();
            if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }

        if (xaType != null && xaXid != null) {
            System.out.println(" ------> " + xaType + " " + xaXid);
        }
    }
}
