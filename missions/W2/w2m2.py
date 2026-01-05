import multiprocessing

def work(name = "Asia"):
    if name:
        print(f"The name of continent is : {name}")

def run():
    proc_list = []

    p1 = multiprocessing.Process(target=work, args=("America",))
    proc_list.append(p1)
    p2 = multiprocessing.Process(target=work, args=("Europe",))
    proc_list.append(p2)
    p3 = multiprocessing.Process(target=work, args=("Asia",))
    proc_list.append(p3)
    p4 = multiprocessing.Process(target=work, args = ("Africa",))
    proc_list.append(p4)

    for proc in proc_list:
        proc.start()
    for proc in proc_list:
        proc.join()


if __name__=="__main__":
    run()