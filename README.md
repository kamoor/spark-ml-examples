This project contains spark-ml algorithms examples

#### Setup
Install scala and spark


#### How to Run
1. Check out the code
2. mvn clean install
3. spark-submit --class <class-youwant-to-run> target/spark-ml-examples-1.0-SNAPSHOT.jar <parameters>
 See each code header to learn more about parameters needed. 

#### Algorithms:

1. Pearson’s Correlation Coefficient (See Correlation.scala)
   Ref: http://www.statisticshowto.com/how-to-compute-pearsons-correlation-coefficients/
2. Spearmans's Correlation Coefficient (See Correlation.scala)  
   Ref: https://statistics.laerd.com/statistical-guides/spearmans-rank-order-correlation-statistical-guide-2.php