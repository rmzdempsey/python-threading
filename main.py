import concurrent.futures
import time
import random
import queue
import threading
from typing import List


class CollectorEmitter:
    def execute(self):
        return 'HUH'


class RMDCollectorExecutor(CollectorEmitter):
    def execute(self):
        time.sleep(random.random())
        return 'RMD'


class ABCCollectorExecutor(CollectorEmitter):
    def execute(self):
        time.sleep(random.random())
        return 'ABC'


class CollectionEventConsumer:
    def consume_event(self, event):
        print(f'eh? {event}')


class RMDCollectionEventConsumer(CollectionEventConsumer):
    def consume_event(self, event):
        if event == 'RMD':
            print(f'ME {event}')


class DEFCollectionEventConsumer(CollectionEventConsumer):
    def consume_event(self, event):
        if event == 'ABC':
            print(f'DEF {event}')


class CollectorPipeline:
    def __init__(self, collector_emitters, consumers):
        self._collector_emitters = collector_emitters
        self._consumers = consumers
        self._queue = queue.Queue()

    def process_collector(self, collector_emitter):
        self._queue.put(collector_emitter.execute())

    def _process_events(self):
        while True:
            item = self._queue.get(block=True)
            for c in self._consumers:
                c.consume_event(item)
            self._queue.task_done()

    def process(self):
        threading.Thread(target=self._process_events, daemon=True).start()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.process_collector, self._collector_emitters)
        self._queue.join()


if __name__ == '__main__':
    collector_emitters_array0: List[CollectorEmitter] = [RMDCollectorExecutor() for x in range(10)]
    collector_emitters_array1: List[CollectorEmitter] = [ABCCollectorExecutor() for x in range(10)]
    collector_emitters_array = collector_emitters_array0 + collector_emitters_array1
    random.shuffle(collector_emitters_array)
    consumers_array = [RMDCollectionEventConsumer(), DEFCollectionEventConsumer()]
    collector_pipeline = CollectorPipeline(collector_emitters_array, consumers_array)
    collector_pipeline.process()

