package org.apache.carbondata.examples.sdk;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TestES {

  public static void main(String[] args) throws IOException {

    Random random = new Random();
    // set the cluster name if you use one different than "elasticsearch"
    Settings settings = Settings.builder().put("cluster.name", "elasticsearch")//.put("index.max_result_window", 1000000)
        //  the client will call the internal cluster state API on those nodes
        // to discover available data nodes
        .put("client.transport.sniff", true).build();
    TransportClient client = new PreBuiltTransportClient(settings)
        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
//    long l1 = System.currentTimeMillis();
//    client.admin().indices().prepareCreate("blocksearch").setSettings(Settings.builder()
//        .put("index.number_of_shards", 10)
//        .put("index.number_of_replicas", 0));
//        //.addMapping("id", "_source", "enabled=false").get();
//
//    BulkRequestBuilder bulkRequest = client.prepareBulk();
//    for (int i = 0; i < 10000000; i++) {
//      XContentBuilder builder =
//          jsonBuilder().startObject()
//              .field("imsi", "imsi122121"+ random.nextInt(Integer.MAX_VALUE))
//              .field("userid", "userid"+random.nextInt(10000000))
//              .field("col1", random.nextInt(1000000))
//              .field("col2", random.nextInt(1000000))
//              .field("col3", random.nextInt(1000000))
//              .field("col4", random.nextInt(1000000))
//              .field("col5", "col1"+random.nextInt(1000000))
//              .field("col6", "col1"+random.nextInt(1000000))
//              .field("col7", "col1"+random.nextInt(1000000))
//              .field("col8", "col1"+random.nextInt(1000000))
//              .field("blockid", "trying out Elasticsearch asass 676").endObject();
//
//      bulkRequest.add(client.prepareIndex("blocksearch", "id", i+"").setSource(builder));
//      if (i % 100000 == 0) {
//        bulkRequest.get();
//        bulkRequest = client.prepareBulk();
//      }
//    }
//
//    if (bulkRequest.numberOfActions() > 0) {
//      bulkRequest.get();
//    }
//    System.out.println("Time take " + (System.currentTimeMillis() - l1));

//    IndexResponse response = bulkRequest.get();

//    GetResponse response = client.prepareGet("tweeter", "tweet", "1").get();
//    DeleteResponse response = client.prepareDelete("tweeter", "tweet", "1").get();
    long l = System.currentTimeMillis();
    SearchResponse response = client.prepareSearch("blocksearch").setTypes("id").setSearchType(
        SearchType.DFS_QUERY_THEN_FETCH)
//        .setFetchSource("blockid", null)
        .setQuery(
        QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("imsi").gt("imsi1221212142338250").lt("imsi1221212199998250"))
            .must(QueryBuilders.rangeQuery("userid").gt("userid4100000").lt("userid8500000"))
            .must(QueryBuilders.rangeQuery("col1").gt(80191).lt(938024))
            .must(QueryBuilders.rangeQuery("col2").gt(4327).lt(736347))
            .must(QueryBuilders.rangeQuery("col3").gt(35451).lt(958889))
            .must(QueryBuilders.rangeQuery("col4").gt(9187).lt(985406))
            .must(QueryBuilders.rangeQuery("col5").gt("col108725").lt("col1808849"))
            .must(QueryBuilders.rangeQuery("col6").gt("col101888").lt("col1896976"))
            .must(QueryBuilders.rangeQuery("col7").gt("col116903").lt("col1917420"))
            .must(QueryBuilders.rangeQuery("col7").gt("col1405294").lt("col186903"))
    ).setSize(1000000).setScroll(new TimeValue(600000)).get();

//    SearchResponse response = client.prepareSearch("bank").setTypes("_doc").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(
//        QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("address", "mill")).must(QueryBuilders.matchQuery("address", "lane"))).get();

//    SearchRequestBuilder searchRequestBuilder =
//        client.prepareSearch("bank").setTypes("_doc").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//            .setQuery(QueryBuilders.matchPhraseQuery("address", "mill lane"));
//
//    System.out.println(searchRequestBuilder);
//    SearchResponse response = searchRequestBuilder.get();
    SearchHit[] hits = null;
    int c = 0;
    do {
      hits = response.getHits().getHits();

      System.out.println("Time taken : " + response.getTook());
      for (SearchHit hit : hits) {
//        System.out.println(hit.getSourceAsMap());
      }
      c += hits.length;
      if (hits.length >= 1000000) {
        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
      } else {
        hits = null;
      }

    } while(hits != null && hits.length != 0); // Zero hits mark the end of the scroll and the while loop.
    System.out.println("Time taken : " + response.getTook());
    System.out.println("Number of hits : " + c);
    System.out.println("Time take " + (System.currentTimeMillis() - l));

//    System.out.println(hits);

    client.close();
  }
}
