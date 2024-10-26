#include "tasksys.h"

#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <iostream>
#include <map>
#include <vector>
#include <thread>

#include <algorithm>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    thread_pool = new std::thread[num_threads];

    keepRunning = true;
    max_task = -1;
    next_task = 0;

    waiting_queue_lock = new std::mutex();
    ready_queue_lock = new std::mutex();
    task_lock = new std::mutex();

    for (int i = 0; i < num_threads_; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadFunc, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    keepRunning = false;
    for (int i = 0; i < num_threads_; i++) {
        thread_pool[i].join();
    }

    delete task_lock;
    delete waiting_queue_lock;
    delete ready_queue_lock;

}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();

}

void TaskSystemParallelThreadPoolSleeping::threadFunc() {
    while (keepRunning) {
        bool shouldDispatch = false;
        ReadyTask currentTask;

        // Lock ready queue and attempt to get the next task
        std::unique_lock<std::mutex> readyLock(*ready_queue_lock);

        if (!ready_queue.empty()) {
            currentTask = ready_queue.front();
            
            if (currentTask.cur_task >= currentTask.num_total_tasks) {
                ready_queue.pop();  // Task batch is completed, remove from queue
            } else {
                ready_queue.front().cur_task++;
                shouldDispatch = true;  // Task is ready to dispatch
            }
        } else {
            // Move eligible tasks from waiting queue to ready queue
            std::lock_guard<std::mutex> waitingLock(*waiting_queue_lock);

            while (!waiting_queue.empty()) {
                auto& nextTask = waiting_queue.top();

                if (nextTask.max_dep_task > max_task) break; // Dependencies not met

                // Move task to ready queue and track progress
                ready_queue.emplace(nextTask.id, nextTask.runnable, nextTask.num_total_tasks);
                task_tracker[nextTask.id] = {0, nextTask.num_total_tasks};
                waiting_queue.pop();
            }
        }
        
        readyLock.unlock();  // Unlock ready queue

        if (shouldDispatch) {
            // Execute the current task
            currentTask.runnable->runTask(currentTask.cur_task, currentTask.num_total_tasks);

            // Update task progress metadata
            std::lock_guard<std::mutex> taskLock(*task_lock);
            auto& taskProgress = task_tracker[currentTask.id];
            if (++taskProgress.first == taskProgress.second) {
                task_tracker.erase(currentTask.id);
                max_task = std::max(currentTask.id, max_task);
            }
        }
    }
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    // return 0;

    TaskID dep = deps.empty() ? -1 : *std::max(deps.begin(), deps.end());

    {
        std::lock_guard<std::mutex> lock(*waiting_queue_lock);
        waiting_queue.push(WaitingTask(next_task, dep, runnable, num_total_tasks));
    }

    return next_task++;    
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    // return;
    while (true) {
        std::lock_guard<std::mutex> lock(*task_lock);
        if (max_task + 1 == next_task) break;
   }
    return;
}
