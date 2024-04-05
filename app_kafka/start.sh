cd /app

apt update
apt install -y python3 python3-pip
pip install confluent-kafka

python3 ./producer.py