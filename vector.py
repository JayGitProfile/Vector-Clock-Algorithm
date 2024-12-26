import json, time, socket, threading
import multiprocessing as mp
from collections import namedtuple
from heapq import heappop, heappush

Event = namedtuple("Event", ["clock", "pid"])
PORTS = [6000, 6005, 6010, 6015]

class ProcessHandler(mp.Process):
    def __init__(obj, id, **kwargs):
        super().__init__(**kwargs)
        obj.id = id
        obj.clock = 4 * [0]
        obj.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def run(obj):
        commTh = ThreadHandler(id=obj.id, clock=obj.clock)
        commTh.start()
        time.sleep(1)

        for _ in range(4):
            obj.perform()
            obj.castEvent()

    def perform(obj):
        print("~~~~~~~~~ doing some operation")
        time.sleep(1)

    def castEvent(obj):
        obj.clock[obj.id] += 1
        msg = dict(pid=obj.id, clock=obj.clock)
        print(f"> Creating P{obj.id}-{msg['clock'][obj.id]}")
        data = json.dumps(msg).encode()
        for port in PORTS:
            obj.socket.sendto(data, ("localhost", port))


class ThreadHandler(threading.Thread):
    def __init__(obj, id, clock, **kwargs):
        super().__init__(**kwargs)
        obj.id = id
        obj.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        obj.socket.bind(("", PORTS[id]))
        obj.clock = clock
        obj.queue = []

    def run(obj):
        while True:
            msg = json.loads(obj.socket.recvfrom(1024)[0].decode())
            obj.queue.append(msg)
            ready_indices = []

            for i, m in enumerate(obj.queue):
                pid = m["pid"]
                if pid != obj.id and m["clock"][pid] != obj.clock[pid] + 1:
                    continue
                for k in range(4):
                    if k != pid and m["clock"][pid] > obj.clock[pid]:
                        continue
                ready_indices.append(i)
                obj.processEvent(m)

            for i in ready_indices[::-1]:
                obj.queue.pop(i)

            for id in range(4):
                obj.clock[id] = max(obj.clock[id], msg["clock"][id])
    
    def processEvent(obj, event):
        pid = event["pid"]
        print("~ P{}: Processed Event P{}-{}".format(obj.id, pid, event["clock"][pid]))


if __name__ == "__main__":
    processes = [ProcessHandler(id=i) for i in range(4)]

    for process in processes:
        process.start()
        time.sleep(1)

    for process in processes:
        process.join()
