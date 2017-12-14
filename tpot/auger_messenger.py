import numpy as np

import redis
import uuid
import pickle
from .pipeline_exports import collect_feature_list, serialize_to_js

class AugerMessenger:
    def __init__(self, conn_info):
        self.conn_info = conn_info
        if self.conn_info:
            self.uid = uuid.uuid4().hex[:15].upper()
            self.r = redis.StrictRedis(host=conn_info['host'], port=conn_info['port'], db=conn_info['db'])

    def send_started(self, cv_num):
        if self.conn_info:
            json = {'started': 1}
            self.r.publish(self.conn_info['channel'], pickle.dumps(json))
            self.r.hset(self.conn_info['channel'], self.uid + '-fold', cv_num)

    def send_scores(self, sklearn_pipeline, features, target, scores):
        if self.conn_info:
            sklearn_pipeline_json = _format_pipeline_json(sklearn_pipeline.steps,features,target)
            sklearn_pipeline_json['score'] = np.nanmean(scores)
            json = {self.uid: sklearn_pipeline_json}
            self.r.publish(self.conn_info['channel'], pickle.dumps(json))

    def send_total_eval(self, total_evals):
        if self.conn_info:
            total_eval_str = pickle.dumps({'total_evaluation': total_evals})
            self.r.publish(self.conn_info['channel'], total_eval_str)

    def send_status_eval(self, status):
        if self.conn_info:
            self.r.publish(self.conn_info['channel'], pickle.dumps({'evaluation_status': status}))
