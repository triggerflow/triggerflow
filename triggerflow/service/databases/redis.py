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

    def get_auth(self, username: str):
        redis_key = '$auth$'
        return self.client.hget(redis_key, username)

    def workspace_exists(self, workspace):
        redis_key = '$workspaces$'
        return self.client.hexists(redis_key, workspace)

    def document_exists(self, workspace, document_id):
        redis_key = '{}-{}'.format(workspace, document_id)
        return self.client.exists(redis_key)

    def key_exists(self, workspace, document_id, key):
        redis_key = '{}-{}'.format(workspace, document_id)
        return self.client.hexists(redis_key, key)

    def set_key(self, workspace, document_id, key, value):
        redis_key = '{}-{}'.format(workspace, document_id)
        self.client.hset(redis_key, key, value)

    def get_key(self, workspace, document_id, key):
        redis_key = '{}-{}'.format(workspace, document_id)
        self.client.hget(redis_key, key)
