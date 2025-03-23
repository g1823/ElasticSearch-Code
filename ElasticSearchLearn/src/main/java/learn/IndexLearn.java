package learn;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import utils.ESClientUtil;

import java.io.IOException;

/**
 * @author: gj
 * @description: TODO
 */
public class IndexLearn {
    public static void main(String[] args) throws IOException {
        IndexLearn javaApiLearn = new IndexLearn();
        RestHighLevelClient client = ESClientUtil.getClient();
        javaApiLearn.createIndex(client);
        // javaApiLearn.getIndex(client);
        //javaApiLearn.deleteIndex(client);
        ESClientUtil.closeClient();
    }

    /**
     * 创建索引
     */
    public void createIndex(RestHighLevelClient client) throws IOException {
        // 注意，索引名称不能存在大写单词，可以用"_"来间隔，否则报错：
        CreateIndexRequest request = new CreateIndexRequest("user");
        // 发送请求，获取相应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        // 输出响应是否被确认
        System.out.println(response.isAcknowledged());
    }

    /**
     * 获取索引信息
     */
    public void getIndex(RestHighLevelClient client) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("index_name");
        GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        // getAliases() 方法返回与该索引关联的所有别名
        System.out.println(getIndexResponse.getAliases());
        // getMappings() 方法返回该索引的字段映射（如字段类型、分析器等）
        System.out.println(getIndexResponse.getMappings());
        // getSettings() 方法返回索引的配置参数（如分片数、副本数等）
        System.out.println(getIndexResponse.getSettings());
    }

    /**
     * 删除索引
     */
    public void deleteIndex(RestHighLevelClient client) throws IOException {
        // 删除索引 - 请求对象
        DeleteIndexRequest request = new DeleteIndexRequest("index_name");
        // 发送请求，获取响应
        AcknowledgedResponse response = client.indices().delete(request,
                RequestOptions.DEFAULT);
        // 操作结果
        System.out.println("操作结果 ： " + response.isAcknowledged());
    }

}
