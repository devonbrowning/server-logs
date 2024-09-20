# Analytics Logs - Producer

## Steps to Start

1. Ensure Kafka is installed and started: `confluent local kafka start`
2. Create the "analytics" topic: `confluent local kafka topic create analytics`
3. Run the following command to start the server logs and pipe them to a Kafka producer using the "server_logs" topic.

```bash
python -u server_logs_producer.py | confluent local kafka topic produce server_logs
```