#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <iostream>
#include <map>
#include <vector>
#include <thread>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
// class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
//     public:
//         TaskSystemParallelThreadPoolSleeping(int num_threads);
//         ~TaskSystemParallelThreadPoolSleeping();
//         const char* name();
//         void run(IRunnable* runnable, int num_total_tasks);
//         TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
//                                 const std::vector<TaskID>& deps);
//         void sync();
// };

struct WaitingTask {
    TaskID id; 
    TaskID max_dep_task; 
    IRunnable* runnable; 
    int num_total_tasks;
    WaitingTask(TaskID id, TaskID dependency, IRunnable* runnable, int num_total_tasks):id{id}, max_dep_task{dependency}, runnable{runnable}, num_total_tasks{num_total_tasks}{};
    bool operator<(const WaitingTask& other) const {
        return max_dep_task > other.max_dep_task;
    }
};

struct ReadyTask {
    TaskID id;
    IRunnable* runnable;
    int cur_task;
    int num_total_tasks;
    ReadyTask(TaskID id, IRunnable* runnable, int num_total_tasks):id(id), runnable(runnable), cur_task{0}, num_total_tasks(num_total_tasks) {}
    ReadyTask(){}
};


class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        int num_threads_;
        std::thread* thread_pool;

        TaskID max_task;         
        TaskID next_task;
        std::map<TaskID, std::pair<int, int> > task_tracker;
                 

        bool keepRunning;         
        

        std::priority_queue<WaitingTask, std::vector<WaitingTask> > waiting_queue;
        std::queue<ReadyTask> ready_queue;

        std::mutex* task_lock; 
        std::mutex* waiting_queue_lock;
        std::mutex* ready_queue_lock;

        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        void threadFunc();
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
