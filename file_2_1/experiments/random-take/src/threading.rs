use std::sync::{Arc, Condvar, Mutex};

/// Lightweight (singleton) task pool for spawning tasks and waiting for them to finish
///
/// This is intended to be used to wrap a single "take" operation.  We create a task
/// for each row group across multiple files and then wait for them all to finish.
///
/// Everything here is done with synchronous APIs.  We could investigate using async for
/// everything but earlier experiments in this vein showed that there was not significant
/// performance improvement.
pub struct TaskPool {
    task_counter: Arc<(Mutex<usize>, Condvar)>,
}

impl TaskPool {
    pub fn new() -> Self {
        Self {
            task_counter: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    pub fn spawn(&self, f: impl FnOnce() + Send + 'static) {
        let mut task_counter = self.task_counter.0.lock().unwrap();
        *task_counter += 1;
        drop(task_counter);

        let task_counter = self.task_counter.clone();
        rayon::spawn(move || {
            f();
            let mut task_counter_count = task_counter.0.lock().unwrap();
            *task_counter_count -= 1;
            if *task_counter_count == 0 {
                task_counter.1.notify_one();
            }
        });
    }

    pub fn join(&self) {
        let (lock, cvar) = &*self.task_counter;
        let mut task_counter = lock.lock().unwrap();
        while *task_counter > 0 {
            task_counter = cvar.wait(task_counter).unwrap();
        }
    }
}
