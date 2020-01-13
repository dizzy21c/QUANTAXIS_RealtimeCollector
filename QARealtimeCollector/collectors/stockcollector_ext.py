import json
import threading
import datetime

from QUANTAXIS import QA_fetch_stock_block_adv
from QUANTAXIS import QA_fetch_get_stock_list 
from QAPUBSUB.consumer import subscriber_routing
from QAPUBSUB.producer import publisher, publisher_routing
from QARealtimeCollector.setting import eventmq_ip
from QUANTAXIS.QAARP.QAUser import QA_User
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAFetch.QATdx_adv import QA_Tdx_Executor
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas

import click

class QARTC_Stock_Ext(QA_Tdx_Executor):
    def __init__(self, code_list = '', block_id = '0', block_name = None, freq = '0'):
        super().__init__(name='QAREALTIME_COLLECTOR_STOCK_EXT')
        self.codelist = code_list.split(',')
        # self.codelist = code_list
        # 默认所有代码, 去除ST
        if self.codelist == ['']:
            code_df = QA_fetch_get_stock_list('tdx')
            code_s = code_df[~code_df.name.str.contains('ST')].code
            self.codelist = code_s.tolist()[:10]
        if block_name is not None:
            self.codelist = QA_fetch_stock_block_adv(blockname= block_name).code
        
        self.block_id = block_id
        self.freq = freq
        self.sub = subscriber_routing(host=eventmq_ip,
                                      exchange='QARealtime_Market', routing_key=block_id)
        self.sub.callback = self.callback
        if freq == '0':
            exchange_name = 'realtime_stock_{}'.format(block_id)
        else:
            exchange_name = 'realtime_stock_{}_{}'.format(block_id, freq)
            
        # self.pub = publisher(
        #     host=eventmq_ip, exchange=exchange_name)
        # self.pub = publisher_routing(host=eventmq_ip, exchange=exchange_name)
        self.pub = publisher_routing(host=eventmq_ip, exchange=exchange_name, routing_key=block_id)
        
        threading.Thread(target=self.sub.start, daemon=True).start()

    def subscribe(self, code):
        """继续订阅

        Arguments:
            code {[type]} -- [description]
        """
        if code not in self.codelist:
            self.codelist.append(code)

    def unsubscribe(self, code):
        self.codelist.remove(code)

    def callback(self, a, b, c, data):
        data = json.loads(data)
        if data['topic'] == 'subscribe':
            print('receive new subscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            import copy
            if isinstance(new_ins, list):
                for item in new_ins:
                    self.subscribe(item)
            else:
                self.subscribe(new_ins)
        if data['topic'] == 'unsubscribe':
            print('receive new unsubscribe: {}'.format(data['code']))
            new_ins = data['code'].replace('_', '.').split(',')

            import copy
            if isinstance(new_ins, list):
                for item in new_ins:
                    self.unsubscribe(item)
            else:
                self.unsubscribe(new_ins)

    def get_data(self):
        if self.freq == '0':
            data, time = self.get_realtime_concurrent(self.codelist)
        # elif self.freq == '1':
        #     data, time = self.get_realtime_concurrent(self.codelist)
        else:
            # data, time = self.get_realtime_concurrent(self.codelist)
            pass
            
        data = QA_util_to_json_from_pandas(data.reset_index())
        self.pub.pub(json.dumps(data), self.block_id)

    def run(self):
        while 1:
            self.get_data()
            import time
            # print(datetime.datetime.now())
            time.sleep(1)

@click.command()
@click.option('--code-list', default='')
@click.option('--block-name', default=None)
@click.option('--block-id', default='0')
@click.option('--freq', default='0')
@click.option('--mode', default='')
def stock_collector_ext(code_list, block_id, block_name, freq, mode):
    if mode == 'add-code':
        pub = publisher_routing(host=eventmq_ip, exchange='QARealtime_Market', routing_key=block_id)
        sendstr = '"topic":"subscribe","code":"{}"'.format(code_list)
        sendstr = ''.join(['{', sendstr, '}'])
        pub.pub(sendstr, routing_key= block_id)
    elif mode == 'del-code':
        pub = publisher_routing(host=eventmq_ip, exchange='QARealtime_Market', routing_key=block_id)
        sendstr = '"topic":"unsubscribe","code":"{}"'.format(code_list)
        sendstr = ''.join(['{', sendstr, '}'])
        pub.pub(sendstr, routing_key= block_id)
    else:
        QARTC_Stock_Ext(code_list, block_id, block_name, freq).start()
    
if __name__ == "__main__":
    stock_collector_ext()
    # r = QARTC_Stock_Ext()
    # r.subscribe('000001')
    # r.subscribe('000002')
    # r.start()

    # r.subscribe('600010')

    # import json
    # import time
    # time.sleep(2)
    # publisher_routing(exchange='QARealtime_Market', routing_key='stock').pub(json.dumps({
    #     'topic': 'subscribe',
    #     'code': '600012'
    # }), routing_key='stock')

    # r.unsubscribe('000001')
