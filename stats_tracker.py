import threading







def do_every(interval, worker_func, iterations = 0):
    if iterations != 1:
        threading.Timer (
            interval,
            do_every, [interval, worker_func, 0 if iterations == 0 else iterations-1]
            ).start ()

    worker_func()


# call print_so every second, 5 times total
# do_every(1, print_so, 5)

# call print_hw two times per second, forever
# do_every(25, print_hw)
