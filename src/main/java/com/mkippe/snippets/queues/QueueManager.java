package com.mkippe.snippets.queues;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.*;

public class QueueManager {
    private final ExecutorService executorService;
    private final ConcurrentMap<String, BlockingQueue<String>> queuePool;

    public QueueManager(RedisTemplate<String, String> redisTemplate) {
        this.executorService = Executors.newCachedThreadPool(); // Adjust the pool size as needed
        this.queuePool = new ConcurrentHashMap<>();
    }

    public void addAction(String action) {
        String queueName = generateQueueName();
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        // Store the queue in the pool
        queuePool.put(queueName, queue);

        // Add the action to the queue
        if (queue.offer(action)) {
            // The element was successfully added to the queue
            // Do additional processing or logging if needed
            System.out.println("task added successfully");
        } else {
            // The queue is full, and the element was not added
            // Handle the situation accordingly
            System.out.println("Queue is full!");
        }


        // Schedule a task to close the queue after a certain amount of time
        scheduleQueueClosure(queueName);
    }

    private String generateQueueName() {
        // Implement your logic to generate a unique queue name
        // You can use UUID or any other strategy based on your requirements
        return "Queue-" + System.currentTimeMillis();
    }

    private void scheduleQueueClosure(String queueName) {
        // Schedule a task to close the queue after a certain amount of time
        executorService.submit(() -> closeQueue(queueName), 10);
    }

    private void closeQueue(String queueName) {
        BlockingQueue<String> queue = queuePool.remove(queueName);

        // Process the actions in the closed queue, e.g., distribute workload
        executorService.submit(() -> processActions(queue));
    }

    private void processActions(BlockingQueue<String> queue) {
        // TODO: Implement your logic to process the actions in the closed queue
        //  i.e distribute workload among other queues or perform other processing
        while (!queue.isEmpty()) {
            String action = queue.poll();
            // Process the action
            System.out.println("Processing action: " + action);
        }
    }

    // Other methods as needed

    public static void main(String[] args) {
        RedisTemplate<String, String> redisTemplate = createRedisTemplate();
        QueueManager queueManager = new QueueManager(redisTemplate);

        // Add actions to the manager
        queueManager.addAction("Action 1");
        queueManager.addAction("Action 2");

        // Close the application or keep it running as needed
    }

    private static RedisTemplate<String, String> createRedisTemplate() {
        // Implement your logic to create and configure RedisTemplate
        // (Assuming you have a configured RedisTemplate)
        // For example, you can use the StringRedisTemplate provided by Spring Data Redis
        return new StringRedisTemplate();
    }
}
