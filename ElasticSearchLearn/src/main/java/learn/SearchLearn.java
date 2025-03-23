package learn;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import utils.ESClientUtil;

import java.io.IOException;
import java.util.Map;

/**
 * @author: gj
 * @description: TODO
 */
public class SearchLearn {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ESClientUtil.getClient();
        SearchLearn searchLearn = new SearchLearn();
        //searchLearn.query(client);
        searchLearn.highLightQuery(client);
        ESClientUtil.closeClient();
    }

    public void query(RestHighLevelClient client) throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();


        // 1、查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);
        SearchResponse queryAllResponse = client.search(request, RequestOptions.DEFAULT);

        // 2、精确查询
        sourceBuilder.query(QueryBuilders.termQuery("name", "zhangsan"));
        request.source(sourceBuilder);
        SearchResponse termQueryResponse = client.search(request, RequestOptions.DEFAULT);

        // 3、分页查询
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 当前页起始索引(第一条数据的顺序号)
        sourceBuilder.from(0);
        // 每页显示多少条 size
        sourceBuilder.size(2);
        request.source(sourceBuilder);
        SearchResponse pageQueryResponse = client.search(request, RequestOptions.DEFAULT);

        // 4、排序
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.sort("name", SortOrder.ASC);
        request.source(sourceBuilder);
        SearchResponse sortQueryResponse = client.search(request, RequestOptions.DEFAULT);

        // 5、过滤字段
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //查询字段过滤
        String[] excludes = {};
        String[] includes = {"name", "age"};
        sourceBuilder.fetchSource(includes, excludes);
        request.source(sourceBuilder);
        SearchResponse filterQueryResponse = client.search(request, RequestOptions.DEFAULT);


        // 6、Bool查询。注意，这里使用了新的查询创建器BoolQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 必须包含
        boolQueryBuilder.must(QueryBuilders.matchQuery("age", "30"));
        // 一定不含
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name", "zhangsan"));
        // 可能包含
        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "男"));
        sourceBuilder.query(boolQueryBuilder);
        request.source(sourceBuilder);
        SearchResponse boolQueryResponse = client.search(request, RequestOptions.DEFAULT);

        // 7、范围查询
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        // 大于等于
        rangeQuery.gte("30");
        // 小于等于
        rangeQuery.lte("40");
        sourceBuilder.query(rangeQuery);
        request.source(sourceBuilder);
        SearchResponse rangeQueryResponse = client.search(request, RequestOptions.DEFAULT);

        // 8、模糊查询
        sourceBuilder.query(QueryBuilders.fuzzyQuery("name","zhangsan").fuzziness(Fuzziness.ONE));
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);


        // 查询匹配
        SearchHits hits = queryAllResponse.getHits();

        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<<========");
    }

    public void highLightQuery(RestHighLevelClient client) throws IOException {
        // 高亮查询
        SearchRequest request = new SearchRequest().indices("user");
        //2.创建查询请求体构建器
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //构建查询方式：高亮查询
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name","zhangsan");
        //设置查询方式
        sourceBuilder.query(termsQueryBuilder);
        //构建高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
        highlightBuilder.postTags("</font>");//设置标签后缀
        highlightBuilder.field("name");//设置高亮字段
        //设置高亮构建对象
        sourceBuilder.highlighter(highlightBuilder);
        //设置请求体
        request.source(sourceBuilder);
        //3.客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.打印响应结果
        SearchHits hits = response.getHits();
        System.out.println("took::"+response.getTook());
        System.out.println("time_out::"+response.isTimedOut());
        System.out.println("total::"+hits.getTotalHits());
        System.out.println("max_score::"+hits.getMaxScore());
        System.out.println("hits::::>>");
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            //打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<<::::");
    }

    //聚合查询
    public void aggsQuery(RestHighLevelClient client) throws IOException {
        // 设置请求索引
        SearchRequest request = new SearchRequest().indices("user");
        // 设置请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 1、最大值
        sourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));
        //设置请求体
        request.source(sourceBuilder);
        //客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 2、分组
        sourceBuilder.aggregation(AggregationBuilders.terms("age_groupby").field("age"));
        //设置请求体
        request.source(sourceBuilder);
        //客户端发送请求，获取响应对象
        SearchResponse response2 = client.search(request, RequestOptions.DEFAULT);

        //4.打印响应结果
        SearchHits hits = response.getHits();
        System.out.println(response);
    }

}
