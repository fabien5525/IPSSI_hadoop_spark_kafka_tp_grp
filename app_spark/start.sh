cd /app 
pip install -r requirements.txt 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ./consumer.py