#
from QAPUBSUB.consumer import subscriber, subscriber_routing
from QAPUBSUB.producer import publisher
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAData.data_resample import QA_data_futuremin_resample, QA_data_futuremin_resample_tb_kq
from QUANTAXIS.QAUtil.QADate_trade import QA_util_future_to_tradedatetime
from QARealtimeCollector.setting import eventmq_ip
import json
import pandas as pd
import numpy as np
import threading
import time


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return str(obj)
        else:
            return super(NpEncoder, self).default(obj)


class QARTC_Resampler_Ext(QA_Thread):
    def __init__(self, block_id='0', freqence='1min', model='tb'):
        super().__init__()
        self.block_id = block_id
        self.freqence = freqence
        # self.sub = subscriber(
            # host=eventmq_ip, exchange='realtime_block_{}'.format(self.block_id))
        self.sub = subscriber_routing(
            host=eventmq_ip, exchange='realtime_block_{}'.format(self.block_id), routing_key = self.block_id)
        self.pub = publisher(
            host=eventmq_ip, exchange='realtime_block_{}_{}'.format(self.block_id, self.freqence))
        self.sub.callback = self.callback
        self.market_data = {}
        self.dt = {}
        self.model = model
        threading.Thread(target=self.sub.start).start()

    def callback(self, a, b, c, data):
        load_datas = json.loads(str(data, encoding='utf-8'))
        # print(load_datas)
        for lastest_data in load_datas:
            code = lastest_data['code']
            # print("code=%s" % code)
            # print(self.market_data.keys())
            if code not in self.market_data.keys():
                self.market_data[code] = []
                self.dt[code] = None
            # print(lastest_data)
            # print(lastest_data['servertime'])
            if self.dt[code] != lastest_data['servertime'] or len(self.market_data[code]) < 1:
                self.dt[code] = lastest_data['servertime']
                # print('new')
                self.market_data[code].append(lastest_data)
            else:
                # print('update')
                self.market_data[code][-1] = lastest_data
            df = pd.DataFrame(self.market_data[code])
            df = df.assign(datetime=pd.to_datetime(df.servertime), code=code, position=0,
                        tradetime=df.datetime.apply(QA_util_future_to_tradedatetime)).set_index('datetime')
            print(df)
            # if self.model == 'tb':
            #     res = QA_data_futuremin_resample_tb_kq(df, self.freqence)
            # else:
            #     res = QA_data_futuremin_resample(df, self.freqence)
            # # print(res)
            # # print(res.iloc[-1].to_dict())
            # self.pub.pub(json.dumps(
            #     res.reset_index().iloc[-1].to_dict(), cls=NpEncoder))

    def run(self):
        while True:
            # print(pd.DataFrame(self.data))
            time.sleep(1)


if __name__ == "__main__":
    QARTC_Resampler_Ext().start()
