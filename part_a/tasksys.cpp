#include "tasksys.h"
#include <iostream>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>

#include <cassert>


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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    num_threads_ = num_threads;
    thread_pool = new std::thread[num_threads];

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete[] thread_pool;
}

// void TaskSystemParallelSpawn::threadFunc(IRunnable* runnable, int num_total_tasks, int start, int step)

void TaskSystemParallelSpawn::threadFunc(IRunnable* runnable, int num_total_tasks) {

    // for(int i=start; i<num_total_tasks; i+=step)
    //     runnable->runTask(i, num_total_tasks);

    int i;

    while(true){
        // if (counter>=num_total_tasks){
        //     break;
        // }
        i = counter++;
        if (i>=num_total_tasks){
            break;
        }
        runnable->runTask(i, num_total_tasks);
    }

}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    counter = 0;

    for (int i = 0; i < num_threads_; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelSpawn::threadFunc, this, runnable, num_total_tasks);
    }

    for (int i = 0; i < num_threads_; i++) {
        thread_pool[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    num_threads_ = num_threads;
    thread_pool = new std::thread[num_threads];

    for(int i=0; i<num_threads; i++){
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::threadFunc, this);
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {

    keepRunning = false;
    for(int i=0; i<num_threads_; i++){
        thread_pool[i].join();
    }
    delete[] thread_pool; 
}

void TaskSystemParallelThreadPoolSpinning::threadFunc(){

    bool check = false;
    std::function<void()> task;

    while(true){
        if(!keepRunning)
            break;
        check = false;
        queueMutex.lock();
        if (!tasks.empty()){
            task = std::move(tasks.front());
            tasks.pop();
            check = true;
        }
        queueMutex.unlock();
        if (check){
            task();
            taskCompleted++;
        }
        cv.notify_one(); // Notify the conditional variable that a task has finished running.
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.

    for (int i = 0; i < num_total_tasks; i++) {
        queueMutex.lock();
        tasks.push([runnable, i, num_total_tasks] {runnable->runTask(i, num_total_tasks);});
        taskCount++;
        queueMutex.unlock();
    }

    std::unique_lock<std::mutex> lock(taskCompletedMutex); 
    cv.wait(lock, [this]{ return (tasks.empty() && (taskCompleted==taskCount)); }); // Wait until task queue is empty and all tasks have finished running.
    lock.unlock();

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

    for(int i=0; i<num_threads; i++){
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadFunc, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    stop = true;
    std::unique_lock<std::mutex> latch(queueMutex);
    cv_start.notify_all();
    latch.unlock();

    for(int i=0; i<num_threads_; i++){
        thread_pool[i].join();
    }
    delete[] thread_pool; 
    
    
}
void TaskSystemParallelThreadPoolSleeping::threadFunc(int i){
    

    while(true){
        std::unique_lock<std::mutex> latch(queueMutex);
        cv_start.wait(latch, [this]{return (stop || !tasks.empty());});

        if(stop){
            break;
        }else{

            ++busy;

            auto task = tasks.front();
            tasks.pop();

            latch.unlock();
            task();
            latch.lock();

            --busy;

            // std::cout<<"Value of busy: "<<busy<<" Number of tasks: "<<tasks.size()<<std::endl;
            cv_end.notify_all();
        }
    }
    
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        std::unique_lock<std::mutex> lock(queueMutex);
        tasks.push([runnable, i, num_total_tasks] {runnable->runTask(i, num_total_tasks);});
        cv_start.notify_one();
    }

    // std::cout<<"Once again we are stuck right before wait"<<std::endl;

    std::unique_lock<std::mutex> latch(queueMutex);
    cv_end.wait(latch, [this]{return (busy==0 && tasks.empty());});

}



TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}