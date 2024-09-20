import os
from typing import Optional

from flask import Flask, request, jsonify
from flask_cors import CORS
import redis
import hashlib
import json

app = Flask(__name__)
CORS(app)

_rdb: Optional[redis.Redis] = None


def init_cache():
    global _rdb
    if url := os.environ.get('KV_URL'):
        url = url.replace('redis://', 'rediss://') + '/0'
    else:
        url = 'redis://localhost:6379/0'
    _rdb = redis.StrictRedis.from_url(url=url, decode_responses=True)


def rdb() -> redis.Redis:
    if _rdb is None:
        init_cache()
    return _rdb


def generate_md5(value):
    return hashlib.md5(json.dumps(value, sort_keys=True).encode('utf-8')).hexdigest()


@app.route('/set', methods=['POST'])
def set_value():
    data = request.json
    group = data.get('group')
    value = data.get('value')

    if not group or value is None:
        return jsonify({'code': -1, 'msg': 'missing group or value'}), 400

    value_id = generate_md5(value)
    value_json = json.dumps(value)

    rdb().hset(f'items:group:{group}', value_id, value_json)

    return jsonify({'code': 0, 'msg': 'success'}), 201


@app.route('/del', methods=['DELETE'])
def delete_value():
    data = request.json
    group = data.get('group')
    value_id = data.get('value_id')

    if not group or value_id is None:
        return jsonify({'code': -1, 'msg': 'missing group or value_id'}), 400

    removed = rdb().hdel(f'items:group:{group}', value_id)

    if removed == 0:
        return jsonify({'code': -1, 'msg': f'value not found in group {group}'}), 404

    return jsonify({'code': 0, 'msg': 'success'}), 200


@app.route('/list', methods=['GET'])
def list_values():
    group = request.args.get('group')

    if not group:
        return jsonify({'code': -1, 'msg': 'missing group'}), 400

    values = rdb().hgetall(f'items:group:{group}')

    values_parsed = {value_id: json.loads(value) for value_id, value in values.items()}

    return jsonify({'code': 0, 'msg': 'success', 'group': group, 'values': values_parsed}), 200


if __name__ == '__main__':
    app.run(debug=True)
