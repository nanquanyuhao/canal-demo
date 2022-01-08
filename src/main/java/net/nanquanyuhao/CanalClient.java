package net.nanquanyuhao;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        // 获取链接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(
                "192.168.235.100", 11111), "example", "", "");

        while (true) {
            // 连接
            canalConnector.connect();
            // 订阅数据库
            canalConnector.subscribe("*.*");

            // 获取数据
            Message message = canalConnector.get(100);
            // 获取 Entry 集合
            List<CanalEntry.Entry> entries = message.getEntries();
            // 判断集合如果为空，则等待后继续拉取
            if (entries.isEmpty()) {
                System.out.println("当次抓取没有数据，休息一会。");
                Thread.sleep(1000);
            } else {
                // 遍历 entries，单条解析
                for (CanalEntry.Entry entry : entries) {
                    // 获取表名
                    String tableName = entry.getHeader().getTableName();
                    // 获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    // 获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();

                    // 判断 entry 类型是否为 rowdata
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        // 反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        // 获取当前时间的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 获取数据集
                        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                        // 遍历 rowDataList，并打印数据集
                        for (CanalEntry.RowData rowData : rowDataList) {
                            JSONObject beforeData = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }

                            JSONObject afterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                afterData.put(column.getName(), column.getValue());
                            }

                            // 数据打印
                            System.out.println("Table:" + tableName + ", EventType:" + eventType + ", Before:" +
                                    beforeData + ", After:" + afterData);
                        }
                    } else {
                        System.out.println("当前操作类型为：" + entryType);
                    }
                }
            }
        }
    }
}
