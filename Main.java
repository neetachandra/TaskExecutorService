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

import openText.MainFirstApproach.QueuedTask;
import openText.MainFirstApproach.Task;
import openText.MainFirstApproach.TaskExecutor;
import openText.MainFirstApproach.TaskGroup;
import openText.MainFirstApproach.TaskType;

public class Main {
	
	public static void main(String[] args) throws InterruptedException{
		Main obj = new Main();
		TaskExecutor executor = obj.new TaskExecutorImpl(5);
		TaskGroup group1 = new TaskGroup(UUID.randomUUID());
		TaskGroup group2 = new TaskGroup(UUID.randomUUID());
		TaskGroup group3 = new TaskGroup(UUID.randomUUID());
		int a =10;
		Task task1 = new Task<Integer>(UUID.randomUUID(), group1, TaskType.READ, ()->{
			Thread.sleep(500);
			return a;
		});
		
		Task task2 = new Task<Integer>(UUID.randomUUID(), group2, TaskType.WRITE, ()->{
			int b = 20;
			Thread.sleep(500);
			return b;
		});
		
		Task task3 = new Task<Integer>(UUID.randomUUID(), group3, TaskType.READ, ()->{
			Thread.sleep(1000);
			return a;
		});
		
		Task task4 = new Task<Integer>(UUID.randomUUID(), group3, TaskType.WRITE, ()->{
			int b = 30;
			Thread.sleep(200);
			return b;
		});
		
		
		Task task5 = new Task<Integer>(UUID.randomUUID(), group3, TaskType.WRITE, ()->{
			int b = 40;
			Thread.sleep(200);
			return b;
		});
		
		
		System.out.println("Task1 " + task1.taskUUID);
		System.out.println("Task2 " + task2.taskUUID);
		System.out.println("Task3 " + task3.taskUUID);
		System.out.println("Task4 " + task4.taskUUID);
		System.out.println("Task5 " + task5.taskUUID);
		
		Future<Integer> result1 = executor.submitTask(task1);
		
		Future<Integer> result2 = executor.submitTask(task2);
		
		Future<Integer> result3 = executor.submitTask(task3);
		
		Future<Integer> result4 = executor.submitTask(task4);
		
		Future<Integer> result5 = executor.submitTask(task5);
		
		
		try {
			result1.get();
			result2.get();
			result3.get();
			result4.get();
			result5.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Thread.sleep(5000);
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
		<T> Future<T> submitTask(Task<T> task);
		void shutdown();
	}
	
	//Implementation of TaskExecutor
	public class TaskExecutorImpl implements TaskExecutor{
		/*@param globalTaskQueue
	     This is universal blocking queue for keeping tasks wrapped in QueuedTask record. It will ensure Fairness among threads.
	     If thread1 is submitted before thread2 then thread1 is guaranteed to be executed before thread2
	     
	     * @param groupChains 
	     * Hashmap is used to keep the groupId and its corresponding compleatablefuture
	     * 
	     * @param dispatcherThread
	     *  This thread is running in the background and take threads from the  taskQueue
	     *  
	     *  @param workerPool
	     *  Executor service provide fixedThreadPool to limit no. of threads in the system
	     */
		
	    private BlockingQueue<QueuedTask<?>> globalTaskQueue = new LinkedBlockingQueue<QueuedTask<?>>(); 
	    private final Map<UUID, CompletableFuture<Void>> groupChains = new ConcurrentHashMap<>();
	    private Thread dispatcherThread; 
	    private final ExecutorService workerPool;
	    
	    

	    public TaskExecutorImpl(int poolSize) {
	        this.workerPool = Executors.newFixedThreadPool(poolSize);
	        dispatcherThread = new Thread(this::dispatchLoop, "dispatcher-thread");
	        dispatcherThread.start();
	    }
	    
	    /*This method is the run method of the Dispatcher thread. It takes tasks from the queue in FIFO order and computes the completableFuture corresponding groupId 
	     * and chains the current task with the lastCompletable future. 
	    */
	    private void dispatchLoop() {
	        while (true) {
	            try {
	                QueuedTask<?> queuedTask = globalTaskQueue.take(); // Take the tasks in FIFO order
	                Task<?> task = queuedTask.task;
	                
	                //Chaining the tasks belonging to same group
	                groupChains.compute(task.taskGroup.groupUUID, (groupId, lastFuture) -> {
	                    if (lastFuture == null || lastFuture.isDone()) {
	                        lastFuture = CompletableFuture.completedFuture(null);
	                    }

	                    return lastFuture.thenRunAsync(() -> {
	                        try {
	                        	System.out.printf("[%s] Executing %s%n",
		                                Thread.currentThread().getName(), queuedTask.task().taskUUID());
	                            Object result = task.taskAction.call(); // Execute task
	                            ((CompletableFuture<Object>) queuedTask.future()).complete(result); // Complete result
	                        } catch (Exception e) {
	                        	((CompletableFuture<Object>) queuedTask.future()).completeExceptionally(e);
	                            System.err.printf("Error in task %s: %s%n", task.taskUUID, e.getMessage());
	                        }
	                    }, workerPool);
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