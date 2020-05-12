package com.example.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Recibe 3 parametros:
 * 1.- ruta HDFS con csv
 * 2.- ruta para ouput adultminor
 * 3.- ruta para ouput integrantes de familias
 * @author VENTURA
 *
 */
public class FunctionExample {

	static class AdultMinorSeparator implements Function<String, String> {

		@Override
		public String call(String input) {

			int age = Integer.parseInt(input.split(",")[2]);
			return (age >= 18 ? "Adult" : "Minor");
		}
	}

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Passing Function to Spark Example using Java");

		sparkConf.setMaster("local[1]");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> guestsRDD = sparkContext.textFile(args[0]);

		JavaRDD<String> adultMinorRDD = guestsRDD.map(new Function<String, String>() {
			@Override
			public String call(String input) {

				int age = Integer.parseInt(input.split(",")[2]);
				return (age >= 18 ? "Adult" : "Minor");
			}
		});

		/* Java Function Passing with named class. */
		// JavaRDD<String> adultMinorRDD = guestsRDD.map(new AdultMinorSeparator());

		/* Java function passing with anonymous inner class */
		JavaPairRDD<String, Integer> pairedRDD = adultMinorRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String input) throws Exception {

				Tuple2<String, Integer> result = new Tuple2<String, Integer>(input, 1);

				return result;
			}
		});

		JavaPairRDD<String, Integer> adultMinorCountsRDD = pairedRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {

						return v1 + v2;
					}

				});

		JavaPairRDD<Integer, Family> familyJavaPairRDD = guestsRDD
				.mapToPair(new PairFunction<String, Integer, Family>() {

					@Override
					public Tuple2<Integer, Family> call(String input) {
						Family family = new Family();
						String[] values = input.split(",");
						int familyId = Integer.parseInt(values[values.length - 1]);
						family.setFamilyId(familyId);
						List<String> memberList = new ArrayList<String>();
						memberList.add(values[1]);
						family.setMembersList(memberList);
						return new Tuple2<Integer, Family>(familyId, family);
					}
				});

		JavaPairRDD<Integer, Family> aggregatedFamilyPairsRDD = familyJavaPairRDD
				.reduceByKey(new Function2<Family, Family, Family>() {
					@Override
					public Family call(Family v1, Family v2) throws Exception {
						Family result = new Family();
						result.setFamilyId(v1.getFamilyId());
						List<String> membersList = new ArrayList<>(
								v1.getMembersList().size() + v2.getMembersList().size());
						membersList.addAll(v1.getMembersList());
						membersList.addAll(v2.getMembersList());
						result.setMembersList(membersList);
						return result;
					}
				});

		adultMinorCountsRDD.saveAsTextFile(args[1]);

		aggregatedFamilyPairsRDD.saveAsTextFile(args[2]);

		sparkContext.stop();

		sparkContext.close();

	}
}
