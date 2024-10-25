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

// TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
//     //
//     // TODO: CS149 student implementations may decide to perform cleanup
//     // operations (such as thread pool shutdown construction) here.
//     // Implementations are free to add new class member variables
//     // (requiring changes to tasksys.h).
//     //
//     // std::cout<<"ABOUT TO NOTIFY ALL THE THREADS FOR THE LAST TIME"<<std::endl;

//     keepRunning = false;
//     cv_start.notify_all();

//     for(int i = 0; i < num_threads_; i++) {
//             thread_pool[i].join(); // Ensure threads finish before destructing
//     }
//     delete[] thread_pool; 
    
// }

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // std::cout<<"ABOUT TO NOTIFY ALL THE THREADS FOR THE LAST TIME"<<std::endl;

    // assert(1==2);

    // Signal to dispatch threads that it's time to wrap up

    std::unique_lock<std::mutex> lock(queueMutex);
    keepRunning = false;
    cv_start.notify_all();
    lock.unlock();

    // Wait for threads to finish before we exit
    for(size_t i = 0; i < num_threads_; i++)
    {
        if(thread_pool[i].joinable())
        {
            // printf("Destructor: Joining thread %zu until completion", i);
            thread_pool[i].join();
        }
    }
    
}
void TaskSystemParallelThreadPoolSleeping::threadFunc(int i){
    
    std::unique_lock<std::mutex> lock(queueMutex);

    do
    {

        cv_end.notify_all();

        // Wait until we have data or a quit signal
        cv_start.wait(lock, [this] {
            return (!tasks.empty() || !keepRunning);
        });

        if (tasks.empty() && !keepRunning)
            break;

        if(!tasks.empty()){
            auto op = std::move(tasks.front());
            tasks.pop();

            // unlock now that we're done messing with the queue
            lock.unlock();

            op();

            lock.lock();
        }
        

    } while(keepRunning);
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // unsigned int n = std::thread::hardware_concurrency();
    // std::cout << n << " concurrent threads are supported.\n";

    for (int i = 0; i < num_total_tasks; i++) {
        // std::cout<<"Pushing task number: "<<i<<std::endl;
        queueMutex.lock();
        // std::cout<<"Inside the lock for: "<<i<<std::endl;
        tasks.push([this, runnable, i, num_total_tasks] {
            runnable->runTask(i, num_total_tasks);
            taskCompleted++;
            // std::cout<<"Completed task: "<<taskCompleted<<std::endl;
            cv_end.notify_all();
        });
        queueMutex.unlock();
        taskCount++;
        cv_start.notify_one();
        // std::cout<<"End of for loop"<<std::endl;
    }
    // std::cout<<std::endl;


    // std::cout<<"Done pushing all the tasks. Task count is: "<<taskCount<<std::endl;
    // std::cout<<"Done pushing all the tasks. Completed so far is: "<<taskCompleted<<std::endl;
    // std::cout<<"Number of tasks left? "<<tasks.size()<<std::endl;

    std::unique_lock<std::mutex> lk(taskCompletedMutex);
    cv_end.wait(lk, [this]{return (tasks.empty() && (taskCompleted==taskCount));});

    
    // cv_end.wait(lk, [this]{return tasks.empty();});
    // taskCompletedMutex.unlock();

    // std::cout<<"Done with run"<<std::endl;
}

// void TaskSystemParallelThreadPoolSleeping::threadFunc(int i){
    
//     std::function<void()> task;
//     bool check = false;

//     while(keepRunning){
//         // std::cout<<"Entered loop"<<std::endl;
//         std::unique_lock<std::mutex> lock(queueMutex);
//         cv_start.wait(lock, [this]{return (!tasks.empty() || !keepRunning);});
//         // cv_start.wait(lock);

//         // std::cout<<"For thread: "<<i<<" Left the conditional wait, and value of keepRunning: "<<keepRunning<<"Tasks empty: "<<tasks.empty()<<std::endl;

//         if (tasks.empty() && !keepRunning)
//         {
//             // std::cout<<"Entered break"<<i<<std::endl;
//             break;
//         }
            

//         // std::cout<<"There are tasks to run"<<std::endl;
        
//         check = false;
//         // std::cout<<"Right after the lock starts: "<<i<<std::endl;
//         if(!tasks.empty())
//         {
//             // std::cout<<"Task queue is not empty: "<<i<<std::endl;
//             task = std::move(tasks.front());
//             tasks.pop();
//             check = true;
//         }
//         // std::cout<<"Right before the ending of the lock: "<<i<<std::endl;
//         lock.unlock();
//         if (check){
//             // std::cout<<"Extracted the task to run"<<std::endl;
//             task();
//             // std::cout<<"Finished running the task"<<std::endl;
//             taskCompleted++;
//         }
//         // std::cout<<"Exited everything"<<std::endl;
//         cv_end.notify_one();
//         // std::cout<<"Finished notifying"<<std::endl;
//         // std::cout<<"Total task count is: "<<taskCount<<std::endl;
//         // std::cout<<"Total completed is: "<<taskCompleted<<std::endl;
//         // std::cout<<"Is tasks empty?"<<tasks.empty()<<std::endl;
//     }
//     // std::cout<<"FINALLY EXITED THE WHILE LOOP"<<std::endl;
// }



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