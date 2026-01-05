from multiprocessing import Queue, Process
import time

def work(pid, tasks_to_complish, tasks_that_are_done):
    while True:
        try:
            task_no = tasks_to_complish.get_nowait()
            time.sleep(0.5)
            tasks_that_are_done.put(f"Task no {task_no} is done by Process-{pid}")
        except:
            break


def run():
    tasks_to_complish = Queue()
    tasks_that_are_done = Queue()

    for i in range(10):
        print(f"Task no {i}")
        tasks_to_complish.put(i)

    proc_list = []
    num_process = 4
    for i in range(1, num_process+1):
        p = Process(target=work, args=(i, tasks_to_complish, tasks_that_are_done))
        proc_list.append(p)

    for proc in proc_list:
        proc.start()
    for proc in proc_list:
        proc.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())

if __name__ == "__main__":
    run()