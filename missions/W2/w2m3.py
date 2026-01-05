from multiprocessing import Process, Queue

def producer(task_q, colors):
    print("pushing items to queue:")
    for idx, item_info in enumerate(colors, start = 1):
        task_q.put(item_info)
        print(f"push item no : {idx} {item_info}")

def consumer(task_q,colors):
    print("popping items from queue:")
    for idx, item_info in enumerate(colors):
        item_info = task_q.get()
        print(f"pop item no : {idx} {item_info}")

def run():
    task_q = Queue()

    colors = ['red', 'green', 'blue', 'black']
    p1 = Process(target=producer, args=(task_q,colors))
    p2 = Process(target=consumer, args=(task_q,colors))

    p1.start()
    p1.join()
    p2.start()


    p2.join()

if __name__ == "__main__":
    run()