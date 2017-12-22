import os
import time
import numpy as np
import uuid
from .pipeline_exports import collect_feature_list, serialize_to_js
from auger_fsclient import AugerFSClient
from .export_utils import export_pipeline

class AugerMessenger:
    def __init__(self, conn_info):
        self.conn_info = conn_info
        self.fs_client = None

        if conn_info:
            self.fs_client = AugerFSClient( os.path.join(conn_info['host'], conn_info['channel']) )
            #self.r = redis.StrictRedis(host=conn_info['host'], port=conn_info['port'], db=conn_info['db'])

    def send_scores(self, sklearn_pipeline, individual, features, target, result, tpot_instance):
        uid = uuid.uuid4().hex[:15].upper()            
        data = {'uid': uid}
        data.update(result)
        data.pop('result', None)

        data['pipeline'] = self._format_pipeline_json(sklearn_pipeline.steps, features, target)
        data['score_mean'] = np.nanmean(data['scores']) if len(data['scores']) else 0
        data['exported_pipeline'] = export_pipeline(individual, tpot_instance.operators, tpot_instance._pset, tpot_instance._imputed, data['score_mean'], True)

        self._send_message_to_pipelines("score", data, uid)

    def send_status_eval(self, status, total_evals=0):
        self._send_message( 'evaluation_status', {'status': status, 'total_evaluation': total_evals})

    def _send_message(self, prefix, msg):
        if self.fs_client:
            file_name = "%s.json"%(prefix)
            self.fs_client.write_json_file(file_name, msg)

    def _send_message_to_pipelines(self, prefix, msg, uid):
        if self.fs_client:
            file_name = "pipelines/%s_%s.json"%(prefix, uid)
            tmp_file_name = os.path.join("pipelines/tmp", file_name)

            self.fs_client.write_json_file(tmp_file_name, msg)
            self.fs_client.rename_file(tmp_file_name, file_name)

    def _format_pipeline_json(self, pipeline,features,target):
        json = {'pipeline_list':[],'func_dict':{}}
        json['feature_matrix'] = collect_feature_list(pipeline,features,target)
        serialize_to_js(pipeline,json['pipeline_list'],json['func_dict'])
        return json
