from multiprocessing import Pool
import time

def work_log(task_info):
    task_name , task_duration = task_info
    print(f"Process {task_name} waiting {task_duration} seconds")
    time.sleep(task_duration)
    print(f"Process {task_name} Finished.")

def run():
    task_list = [('A',5),('B',2),('C',1),('D',3)]

    with Pool(processes=2) as pool:
        result = pool.map(work_log, task_list)

if __name__ == "__main__":
    run()
