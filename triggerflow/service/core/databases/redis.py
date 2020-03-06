import redis


class RedisClient:
    max_retries = 15

    def __init__(self, host: str, port: int, password: str=None):
        self.client = redis.StrictRedis(host=host, port=port, password=password)

    def get_conn(self):
        return self.client

    def put(self, workspace: str, document_id: str, data: dict):
        redis_key = '{}-{}'.format(workspace, document_id)
        self.client.hmset(redis_key, data)

    def get(self, workspace: str, document_id: str):
        redis_key = '{}-{}'.format(workspace, document_id)
        return self.client.hgetall(redis_key)

    def delete(self, workspace: str, document_id: str):
        redis_key = '{}-{}'.format(workspace, document_id)
        self.client.delete(redis_key)
