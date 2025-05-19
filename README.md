# Multiprocessing Module

- Concurrent processes working on a time-consuming batch of tasks
- Each worker takes tasks from a common queue
- Each worker is assigned to a requestor.  When grabbing a task, a worker looks
    for the first task from its requestor, and if there isn't any, the worker opts for
    the first task in the queue.
- New workers are launched in real time if calculation runs behind schedule
- Inter-process communication via named unix sockets
- Tests are in tests/.  These scripts also serve as examples.

Basic usage:

    my_worker.py:
        #!/bin/python
        from swarm.worker import run


        class Procesor:
            def initialize(self, *init_data):
                # Initialize your task processor, optional...

            def __call__(self, *task):
                # Process task...
                return result


        if __name__ == '__main__':
            processor = Processor()
            run(task_processor=processor.__call__, initializer=processor.initialize)     

    # app.py
        from swarm.client import Socket, Sprint, Pool, BatchManager
        
        # Run a pool of workers with target time of completion 1 minute from now...
        sprint = Sprint()
        with Pool() as pool:
            pool.submit_sprint(sprint)
            sprint.submit_tasks(tasks)
            results = sprint.yield_results()


        with Pool('my_worker.py') as pool:
            # Prepare init_data, if needed, and a list of tasks
            batch_manager = BatchManager(pool, tasks, init_data=init_data)
            batch = BatchManager.batch
            batch.start(tta=time() + 60)
            results = batch.list_results() # Results will be listed in the same order as tasks.

        # Low-level communication with a worker is also possible:
        worker = Socket(pid) # pid of the worker process
        worker.connect()
        worker.initialize(init_data)
        # task_id is an identifier, task is the actual data to be sent for processing
        res_id, result = worker.do_task([task_id, task])  
        assert ret_id == task_id
