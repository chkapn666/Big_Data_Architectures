from celery import Celery

# BEST PRACTICE: name the app in the exact same way as you name the file (and vice versa)
app = Celery('my_app0', broker = 'amqp://localhost')  # the app is configured to use the RabbitMQ message queue
# broker (based on the amqp protocol) running on localhost

# 0. I open a terminal and start rabbitmq locally by executing the following command on a terminal: "rabbitmq-server"
# 1. I open a terminal and start redis locally by executing "redis-server"
# 2. I open a terminal and point at the concurrent execution of tasks defined in the current app's script by 
# executing the command "celery -A my_app0 worker -l INFO"
# 3. I write a {app_name}_x.py script utilizing the distributed tasks and execute python ...x.py

# Tasks that can be executed are defined with the '@app.task' decorator
@app.task
def add(x, y):
    return x + y

@app.task
def sub(x, y):
    return x - y

@app.task
def mul(x, y):
    return x * y

@app.task
def div(x, y):
    return x // y


# When i first delegate this app to a worker via
# "celery -A my_app0 worker -l INFO"

# # I see
#  -------------- celery@ChristoorossAir v5.4.0 (opalescent)
# --- ***** ----- 
# -- ******* ---- macOS-14.5-arm64-arm-64bit 2025-03-13 09:43:31
# - *** --- * --- 
# - ** ---------- [config]
# - ** ---------- .> app:         my_app0:0x1045ea650  # 
# - ** ---------- .> transport:   amqp://guest:**@localhost:5672//
# - ** ---------- .> results:     disabled://
# - *** --- * --- .> concurrency: 8 (prefork)  # this is the 'concurrency level' - the # of forks a process can do - by default, it is equal to the number of CPUs/cores on our machine
# 'parent' process -> program in execution, visible to the OS
# 'child' fork -> exact replica of the process, results from a direct invocation to the OS
# So 'concurrency level' equals the # of times that a worker unit can fork sub-processes.
# The point of forks is creating clones of processes facilitated by mechanisms that help us differentiate between parent and child processes.
# I we're in the child process/fork, we want to overlay our code with new code.
# When we call a fork => the parent process gets the PID of the child, and the child gets a PID of zero.
# -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
# --- ***** ----- 
#  -------------- [queues]
#                 .> celery           exchange=celery(direct) key=celery