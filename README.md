# TaskExecutorService
Task Executor service to execute the task concurrently.
This service is required to implement following behaviour:
1. Tasks can be submitted concurrently and task submission should not the submitter.
2. Task should run asynchronously and concurrely with restricted concurrency.
3. Tasks resuls should be available at the Future.
4. Order of the tasks should be preserved.
5. Tasks beloging to same group mus not concurrenly.


Approach Used for the Task Executor service:

Two main data structure being used here are :
BlockingQueue named as taskQueue and HashSet named as runningGroups.

Whenever a task is being submitted to the Task Executor service, it is enqueued into the taskQueue. runningGroups contain the groupIds of all the groups for which there is a thread running. 

There is a dispatcher thread which is running in the backgroud to dequeue the taskQueue to get a task and check if the task belong to the groupIds present in the runningGroup thread. If yes, it again enqueues that task in the queue at the tail of queue. If no, it add the groupId of the task in the runningGroups HashSet and submit the task's taskAction in the workerPool executor service. This worker pool ExecutorService is a fixSizeThreadPool using which we are restricting the concurrency in the system.

After submitting the taskAction in the workerPool executor service, result of the taskAction is retured in the CompletableFuture. 
