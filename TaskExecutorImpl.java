package openText;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import openText.Main.QueuedTask;
import openText.Main.Task;
import openText.Main.TaskExecutor;

public class TaskExecutorImpl implements TaskExecutor{
    // This is universal blocking queue for keeping threads. It will ensure Fairness among threads.
    //If thread1 is submitted before thread2 then thread1 is guarenteed to be executed before thread2
    private BlockingQueue<QueuedTask<?>> taskQueue = new LinkedBlockingQueue<QueuedTask<?>>(); 
    // Hashmap is used to keep the groupId for which thread is running
    private Set<UUID> runningGroups = ConcurrentHashMap.newKeySet();
    // This thread is running in the background and take threads from the  
    private Thread dispatcherThread; 
    //Executor service to limit no. of threads
    private final ExecutorService workerPool;
    

    public TaskExecutorImpl(int poolSize) {
        this.workerPool = Executors.newFixedThreadPool(poolSize);
        dispatcherThread = new Thread(this::dispatchLoop, "dispatcher-thread");
        dispatcherThread.start();
    }
    
    private void dispatchLoop() {
        while (true) {
            try {
                QueuedTask<?> queuedTask = taskQueue.take(); // Take the tasks in FIFO order
                synchronized (runningGroups) {
                    if (runningGroups.contains(queuedTask.task().taskGroup().groupUUID())) {
                        // Requeue task if its group is still running
                        taskQueue.offer(queuedTask);
                        Thread.sleep(10); // Avoid tight loop
                        continue;
                    }
                    runningGroups.add(queuedTask.task().taskGroup().groupUUID()); // Mark group as running
                }

                // Dispatch to worker pool
                workerPool.submit(() -> {
                    try {
                        System.out.printf("[%s] Executing %s (Group: %s)%n",
                                Thread.currentThread().getName(), queuedTask.task().taskUUID(), queuedTask.task().taskGroup().groupUUID());
                        Object result = queuedTask.task().taskAction().call();
                        //We have to use completableFuture here as this is dispatcher thread and not returning so future will not help.
                        ((CompletableFuture<Object>) queuedTask.future()).complete(result);
                    } catch (Exception ex) {
                    	((CompletableFuture<Object>) queuedTask.future()).completeExceptionally(ex);
                    } finally {
                        synchronized (runningGroups) {
                            runningGroups.remove(queuedTask.task().taskGroup().groupUUID()); // Release group lock
                        }
                    }
                });

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

	

	@Override
	public <T> CompletableFuture<T> submitTask(Task<T> task) {
		CompletableFuture<T> completableFuture = new CompletableFuture<T>();
		//Wrapping the task with QueuedTask
		QueuedTask<T> queue = new QueuedTask<T>(task, completableFuture);
		//Enqueueing the queue with the queued task
		taskQueue.offer(queue);
		//Completable future in the QueuedTask contains result of the taskAction
		return completableFuture;
	}
	
	//This method closes the workerPool executor service and interupts the dispatcher thread otherwise workerPool executor service will be live and dispatcher thread will 
	//continue running in the background
	public void shutdown() {
		workerPool.shutdown();
		dispatcherThread.interrupt();
    }
		 

}
