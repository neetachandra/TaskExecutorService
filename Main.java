package openText;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import openText.Main.QueuedTask;
import openText.Main.Task;
import openText.Main.TaskExecutor;
import openText.Main.TaskGroup;
import openText.Main.TaskType;

public class Main {
	
	public static void main(String[] args) throws InterruptedException{
		Main obj = new Main();
		TaskExecutor executor = obj.new TaskExecutorImpl(3);
		TaskGroup group1 = new TaskGroup(UUID.randomUUID());
		TaskGroup group2 = new TaskGroup(UUID.randomUUID());
		TaskGroup group3 = new TaskGroup(UUID.randomUUID());
		Task task1 = new Task<String>(UUID.randomUUID(), group1, TaskType.READ, ()->{
			Thread.sleep(500);
			return "task1 complete";
		});
		Task task2 = new Task<String>(UUID.randomUUID(), group2, TaskType.READ, ()->{
			Thread.sleep(1000);
			return "task2 complete";
		});
		
		Task task3 = new Task<String>(UUID.randomUUID(), group2, TaskType.WRITE, ()->{
			Thread.sleep(200);
			return "task3 complete";
		});
		
		Task task4 = new Task<String>(UUID.randomUUID(), group3, TaskType.WRITE, ()->{
			Thread.sleep(500);
			return "task4 complete";
		});
		
		
		System.out.println("Task1 " + task1.taskUUID);
		System.out.println("Task2 " + task2.taskUUID);
		System.out.println("Task3 " + task3.taskUUID);
		System.out.println("Task4 " + task4.taskUUID);
		
		CompletableFuture<String> result1 = executor.submitTask(task1);
		
		CompletableFuture<String> result2 = executor.submitTask(task2);
		
		CompletableFuture<String> result3 = executor.submitTask(task3);
		
		CompletableFuture<String> result4 = executor.submitTask(task4);
		
		
		
			result1.thenAccept(result -> System.out.println(result));
			result2.thenAccept(result -> System.out.println(result));
			result3.thenAccept(result -> System.out.println(result));
			result4.thenAccept(result -> System.out.println(result));
		
		Thread.sleep(2000);
		executor.shutdown();
		
	}
	

	//Enumeration of task types.

	public enum TaskType {
		READ, WRITE;
	}
	

	public interface TaskExecutor {
		/**
     * Submit new task to be queued and executed.
     *
     * @param task Task to be executed by the executor. Must not be null.
     * @return Future for the task asynchronous computation result.
     */
		<T> CompletableFuture<T> submitTask(Task<T> task);
		void shutdown();
	}
	
	//Implementation of TaskExecutor
	public class TaskExecutorImpl implements TaskExecutor{
		/*@param taskQueue
	     This is universal blocking queue for keeping tasks wrapped in QueuedTask record. It will ensure Fairness among threads.
	     If thread1 is submitted before thread2 then thread1 is guaranteed to be executed before thread2
	     
	     * @param runningGroups 
	     * Hashmap is used to keep the groupId for which thread is running
	     * 
	     * @param dispatcherThread
	     *  This thread is running in the background and take threads from the  taskQueue
	     *  
	     *  @param workerPool
	     *  Executor service provide fixedThreadPool to limit no. of threads in the system
	     */
		
	    private BlockingQueue<QueuedTask<?>> globalTaskQueue = new LinkedBlockingQueue<QueuedTask<?>>(); 
	    private Set<UUID> runningGroups = ConcurrentHashMap.newKeySet();
	    private Thread dispatcherThread; 
	    private final ExecutorService workerPool;
	    
	    

	    public TaskExecutorImpl(int poolSize) {
	        this.workerPool = Executors.newFixedThreadPool(poolSize);
	        dispatcherThread = new Thread(this::dispatchLoop, "dispatcher-thread");
	        dispatcherThread.start();
	    }
	    
	    /*This method is the run method of the Dispatcher thread. It takes tasks from the queue in FIFO order and Submits
	    them into workerPool if corresponding group is not running  
	    If the group corresponding to the task is running then it inserts the tast at the end of the queue 
	    */
	    private void dispatchLoop() {
	        while (true) {
	            try {
	                QueuedTask<?> queuedTask = globalTaskQueue.take(); // Take the tasks in FIFO order
	                synchronized (runningGroups) {
	                    if (runningGroups.contains(queuedTask.task().taskGroup().groupUUID())) {
	                        // Requeue task if its group is still running
	                        globalTaskQueue.offer(queuedTask);
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
			globalTaskQueue.offer(queue);
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


	/**
   * Representation of computation to be performed by the {@link TaskExecutor}.
   *
   * @param taskUUID Unique task identifier.
   * @param taskGroup Task group.
   * @param taskType Task type.
   * @param taskAction Callable representing task computation and returning the result.
   * @param <T> Task computation result value type.
   */
	public record Task<T>(UUID taskUUID, TaskGroup taskGroup, TaskType taskType, Callable<T> taskAction) 
	{
		public Task{
			if(taskUUID==null || taskGroup==null || taskType==null || taskAction==null)
				throw new IllegalArgumentException("All parameters must not be null");
		}
	}
	
	/**
   * Task group.
   *
   * @param groupUUID Unique group identifier.
   */
	public record TaskGroup(UUID groupUUID) {
		public TaskGroup {
			if(groupUUID==null) {
				throw new IllegalArgumentException("All parameters must not be null");
			}
			
		}

	}
	
	/*
	 * QueueTask is a wrapper for Task as we want result of the task is associated with Task. 
	 * 
	 * @param task represents a task record
	 * @param future represents result of the taskAction 
	 * 
	 */
	
	public record QueuedTask<T>(Task<T> task, CompletableFuture<T> future)
	{
		public QueuedTask{
			if(task==null || future==null)
				throw new IllegalArgumentException("All parameters must not be null");
		}
	}

}
