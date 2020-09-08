package com.fannan.sparkElasticsearch;

import javax.annotation.concurrent.Immutable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableMap;

import com.fannan.sparkElasticsearch.model.MyModel;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Hello world!
 *
 */
public class App implements scala.Serializable {
	private static final ObjectMapper objMapper = new ObjectMapper();

	public static void main(String[] args) {
			SparkConf conf = new SparkConf().setAppName("SPARK").setMaster("local[3]").set("es.index.auto.create", "true")
					.set("spark.driver.allowMultipleContexts","true").set("es.nodes", "localhost:9200");
	
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			
			JavaRDD<String> rddData = sparkContext.textFile("/home/ebdesk/eclipse-workspace/sparkElasticsearch/src/main/java/com/fannan/sparkElasticsearch/americaPlace.csv")
					.map(new Function<String, String>() {

						@Override
						public String call(String v1) throws Exception {
							String json = null;
							try {
								String[]data = v1.split(",");
								MyModel model = new MyModel(data[0], data[0], data[1], Integer.valueOf(data[2]), data[3],
										Integer.valueOf(data[4]), Integer.valueOf(data[5]), Integer.valueOf(data[6]), data[7],
										data[8], Integer.valueOf(data[9]), data[10], data[11]);
								json = objMapper.writeValueAsString(model);
							}catch (Exception e) {
								e.printStackTrace();
							}
							return json;
						}
					});
			try {
				System.out.println("saving.....");
				JavaEsSpark.saveJsonToEs(rddData, "spark/_doc",ImmutableMap.of("es.mapping","id"));
			}catch(Exception e) {
				e.getMessage();
			}
	}
}
