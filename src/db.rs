use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, broadcast};
use tokio::time;
use tokio::time::{Duration, Instant};

// 定义一个结构体，用于表示数据库实例
#[derive(Debug, Clone)]
pub(crate) struct Db {
    // 共享状态，包含数据库的所有数据和状态
    shared: Arc<Shared>,
}


#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

impl Shared {
    // 清除过期的键
    fn purge_expired_keys(&self) -> Option<Instant> {
        // 获取互斥锁，以访问状态
        let mut state = self.state.lock().unwrap();

        // 如果数据库已关闭，则不进行任何操作
        if state.shutdown {
            return None;
        }

        // 获取状态的可变引用
        let state = &mut *state;

        // 获取当前时间
        let now = Instant::now();

        // 遍历过期时间映射，移除过期的键
        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            // 如果当前时间小于过期时间，则返回下一个过期时间
            if when > now {
                return Some(when);
            }
            // 从 entries 中移除过期的键
            state.entries.remove(key);
            // 从 expirations 中移除过期的键
            state.expirations.remove(&(when, id));
        }

        None
    }

    // 检查数据库是否已关闭
    fn is_shutdown(&self) -> bool {
        // 获取互斥锁，以访问状态
        self.state.lock().unwrap().shutdown
    }
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,

    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    expirations: BTreeMap<(Instant, u64), String>,

    next_id: u64,

    shutdown: bool,
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

#[derive(Debug)]
struct Entry {
    id: u64,
    data: Bytes,
    expires_at: Option<Instant>,
}

impl Db {
    // 创建一个新的 Db 实例
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            // 初始化状态，包含一个空的哈希表、一个空的发布订阅哈希表、一个空的过期时间映射、下一个 ID 为 0，以及关闭状态为 false
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            // 创建一个新的 Notify 实例，用于通知后台任务
            background_task: Notify::new(),
        });

        // 启动一个异步任务，用于清除过期的键
        tokio::spawn(purge_expired_tasks(shared.clone()));

        // 返回一个新的 Db 实例
        Db { shared }
    }

    // 获取指定键的值
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取互斥锁，以访问状态
        let state = self.shared.state.lock().unwrap();
        // 从 entries 中获取指定键的值，并返回其克隆
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    // 设置指定键的值，并可选地设置过期时间
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        // 获取互斥锁，以访问状态
        let mut state = self.shared.state.lock().unwrap();

        // 获取下一个 ID
        let id = state.next_id;
        // 增加下一个 ID
        state.next_id += 1;

        // 标记是否需要通知后台任务
        let mut notify = false;

        // 处理过期时间
        let expires_at = expire.map(|duration| {
            // 计算过期时间
            let when = Instant::now() + duration;
            // 检查是否需要通知后台任务
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            // 将过期时间插入到 expirations 中
            state.expirations.insert((when, id), key.clone());
            // 返回过期时间
            when
        });

        // 插入或更新键值对
        let prev = state.entries.insert(key, Entry {
            // 设置 ID
            id,
            // 设置数据
            data: value,
            // 设置过期时间
            expires_at,
        });

        // 如果之前存在该键，则从 expirations 中移除
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }

        // 释放互斥锁
        drop(state);

        // 如果需要通知后台任务，则进行通知
        if notify {
            self.shared.background_task.notified();
        }
    }

    // 订阅指定键的发布订阅频道
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 获取互斥锁，以访问状态
        let mut state = self.shared.state.lock().unwrap();

        // 检查键是否已经存在于 pub_sub 中
        match state.pub_sub.entry(key) {
            // 如果键已经存在，则返回订阅者
            Entry::Occupied(e) => e.get().subscribe(),
            // 如果键不存在，则创建一个新的发布订阅频道，并返回订阅者
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    // 发布指定键的值到发布订阅频道
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        // 获取互斥锁，以访问状态
        let state = self.shared.state.lock().unwrap();
        // 从 pub_sub 中获取指定键的发送者，并发送值
        state
            .pub_sub
            .get(key)
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }
}


// 为 Db 结构体实现 Drop 特征，用于在实例被销毁时执行清理操作
impl Drop for Db {
    fn drop(&mut self) {
        // 检查当前共享实例的强引用计数是否为 2
        if Arc::strong_count(&self.shared) == 2 {
            // 获取互斥锁，以访问状态
            let mut state = self.shared.state.lock().unwrap();
            // 设置状态为关闭
            state.shutdown = true;
            // 释放互斥锁
            drop(state);
            // 通知后台任务
            self.shared.background_task.notified();
        }
    }
}


// 定义一个异步函数，用于清除过期的任务
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 只要数据库未关闭，就一直循环执行清除任务
    while !shared.is_shutdown() {
        // 调用 `purge_expired_keys` 方法获取下一个过期时间
        if let Some(when) = shared.purge_expired_keys() {
            // 使用 `tokio::select!` 宏等待 `sleep_until` 或 `background_task` 通知
            tokio::select! {
                // 等待直到指定的时间 `when`
                _=time::sleep_until(when)=>{}
                // 或者等待 `background_task` 被通知
                _=shared.background_task.notified()=>{}
            }
        } else {
            // 如果没有找到过期时间，则等待 `background_task` 通知
            shared.background_task.notified().await;
        }
    }
}
