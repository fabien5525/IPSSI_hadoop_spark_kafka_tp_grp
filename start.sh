docker compose down -v
docker compose up -d

# Kafka
docker compose exec kafka bash -c 'cd app && ./start.sh'

# Spark
docker compose exec spark-master bash -c 'cd app && ./start.sh'

# Hadoop
docker compose exec hadoop-namenode bash -c 'hadoop fs -ls /data'