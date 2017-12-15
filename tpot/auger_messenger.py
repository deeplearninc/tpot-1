import numpy as np

import redis
import uuid
import pickle
from .pipeline_exports import collect_feature_list, serialize_to_js

class AugerMessenger:
    def __init__(self, conn_info):
        self.conn_info = conn_info
        self.uid = uuid.uuid4().hex[:15].upper()

        if self.conn_info:
            self.r = redis.StrictRedis(host=conn_info['host'], port=conn_info['port'], db=conn_info['db'])

    def send_started(self, cv_num):
        self._send_message({'started': 1})
        #self.r.hset(self.conn_info['channel'], self.uid + '-fold', cv_num)

    def send_scores(self, sklearn_pipeline, features, target, scores):
        sklearn_pipeline_json = self._format_pipeline_json(sklearn_pipeline.steps,features,target)
        sklearn_pipeline_json['score'] = np.nanmean(scores)
        self._send_message({self.uid: sklearn_pipeline_json})

    def send_total_eval(self, total_evals):
        self._send_message({'total_evaluation': total_evals})

    def send_status_eval(self, status):
        self._send_message({'evaluation_status': status})

    def _send_message(self, msg):
        print(msg)
        if self.conn_info:
            self.r.publish(self.conn_info['channel'], pickle.dumps(msg))

    def _format_pipeline_json(self, pipeline,features,target):
        json = {'pipeline_list':[],'func_dict':{}}
        json['feature_matrix'] = collect_feature_list(pipeline,features,target)
        serialize_to_js(pipeline,json['pipeline_list'],json['func_dict'])
        return json
