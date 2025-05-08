# Multiprocessing Module

- Concurrent processes working on a time-consuming batch of tasks
- Each worker takes tasks from a common queue
- New workers are launched in real time if calculation runs behind schedule
- Inter-process communication via named unix sockets
- Tests are in tests/.  These scripts also serve as examples.

Basic usage:

    # my_worker.py:
        #!/bin/python
        from workerpool.worker import run


        class TaskProcessor:
            def __init__(self, ...)
                # the usual

            def initialize(self, *init_data):
                # Initialize your task processor, optional...

            def __call__(self, task):
                # Process task...
                return result


        if __name__ == '__main__':
            task_processor = TaskProcessor()
            run(task_processor=task_processor, initializer=task_processor.initialize)     

    
    # app.py
        from time import time
        from workerpool.client import, Pool, Batch, BatchManager
        
        # Run a pool of workers with target time of completion 1 minute from now...
        with Pool(my_worker.py') as pool:
            # Prepare init_data, if needed, and a list of tasks
            batch_manager = BatchManager(pool, tasks, init_data=init_data)
            batch = BatchManager.batch
            batch.start(tta=time() + 60)
            results = batch.get_results() # Results will be listed in the same order as tasks.

        # Low-level communication with a worker is also possible:
        worker = Socket(pid) # pid of the worker process
        worker.connect()
        assert worker.initialize(init_data)
        # task_id is an identifier, task is the actual data to be sent for processing
        res_id, result = worker.do_task([task_id, task])  
        assert ret_id == task_id
