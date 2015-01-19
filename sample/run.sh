rm -rf result
rm -rf out 
mvn clean package && ../bin/spark-submit --class "SimpleApp" --master local[*] target/simple-project-1.0.jar
