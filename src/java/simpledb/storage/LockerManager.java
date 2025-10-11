package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.ArrayDeque;


public class LockerManager {
    enum LockType { SHARED, EXCLUSIVE}

    // 等待图：谁在等谁（有向边）
    private Map<TransactionId, Set<TransactionId>> waitFor = new HashMap<>();

    // 正在等待哪个Page
    private Map<TransactionId, PageId> waitingOn = new HashMap<>();

    private static class LockState {
        LockType type = null;
        Set<TransactionId> holders = new HashSet<>();
    }

    // PageId -> LockState
    private final Map<PageId, LockState> lockTable = new HashMap<>();

    // TransactionId -> Set<PageId>
    private final Map<TransactionId, Set<PageId>> transactionLocks = new HashMap<>();

    // 能否授予锁
    private boolean canGrantLock(LockState state, LockType request, TransactionId tid) {
        // 空闲页，没人持锁
        if (state == null || state.type == null || state.holders.isEmpty()) return true;

        // 请求S锁
        if (request == LockType.SHARED) {
            // 当前是S
            if (state.type == LockType.SHARED) {
                return true;
            }
            // 当前是X，但持有者是自己
            if (state.type == LockType.EXCLUSIVE && state.holders.contains(tid)) {
                return true;
            }
            return false;
        }
        // 请求X锁
        else {
            // 当前是S，但持有者只有自己
            if (state.type == LockType.SHARED && state.holders.size() == 1 && state.holders.contains(tid)) {
                return true;
            }
            // 当前是X，但持有者是自己
            if (state.type == LockType.EXCLUSIVE && state.holders.contains(tid)) {
                return true;
            }
            return false;
        }

    }

    // 把 Permissions 翻译成锁类型
    private static LockType toLockType(Permissions perm) {
        if (perm == Permissions.READ_ONLY) {
            return LockType.SHARED;
        } else {
            return LockType.EXCLUSIVE;
        }
    }

    // tid：当前正在请求锁的事务 ID
    // state：当前页的锁状态
    // want：当前事务想要的锁类型
    private void addWaitEdges (TransactionId tid, LockState state, LockType want) {
        // 空/无人持锁 → 不应进入这里（canGrant 已经会 true），但保险起见直接返回
        if (state == null || state.type == null || state.holders.isEmpty()) return;

        // 需要把当前事务 tid 在等待图中的旧出边清理掉，重新建一个新的空集合来装它这次的等待关系
        Set<TransactionId> holders = new HashSet<>();
        // 若 tid 原来不存在：等价于插入新键；若 tid 已经存在：等价于覆盖 -> 清空旧的等待关系
        waitFor.put(tid, holders);

        // 若当前事务想要S锁
        if (want == LockType.SHARED) {
            // 当前页的锁是X锁
            if (state.type == LockType.EXCLUSIVE){
                // 当前tid指向持有X锁的业务（不指自己）
                for (TransactionId holder : state.holders) {
                    if (!holder.equals(tid)) {
                        holders.add(holder);
                    }
                }
            }
        }
        // 若当前事务想要X锁
        else {
            // 当前页的锁是S锁
            if (state.type == LockType.SHARED) {
                // 持有S锁的还有别的事务（不指自己）
                for (TransactionId holder : state.holders) {
                    if (!holder.equals(tid)) {
                        holders.add(holder);
                    }
                }
            }
            // 当前页的锁是X锁
            else{
                // 当前tid指向持有X锁的业务（不指自己）
                for (TransactionId holder : state.holders) {
                    if (!holder.equals(tid)) {
                        holders.add(holder);
                    }
                }

            }
        }

    }

    // 检测从 start 出发，能否回到 start
    private boolean hasCycle(TransactionId start) {
        // 模拟 DFS 的递归调用栈
        ArrayDeque<TransactionId> stack = new ArrayDeque<>();
        // 记录访问过的节点
        HashSet<TransactionId> seen = new HashSet<>();

        stack.push(start);

        // 当栈不为空时，说明还有待探索的事务
        while (!stack.isEmpty()) {
            // 取出当前要处理的事务（相当于 DFS 里的当前节点）
            TransactionId current = stack.pop();

            // 找出当前事务 current 正在等待的所有事务（current → outs）
            Set<TransactionId> outs = waitFor.get(current);

            // 如果 current 没有在等任何事务，跳过
            if (outs == null || outs.isEmpty()) {
                continue;
            }

            // 遍历 current 的所有出边（也就是它正在等待的每一个事务）
            for (TransactionId neighbor : outs) {
                // 如果发现了回到 start 的路径，说明有环
                if (neighbor.equals(start)) {
                    return true;
                }
                // 如果 neighbor 没被访问过，加入栈和 seen 集合
                if (!seen.contains(neighbor)) {
                    seen.add(neighbor);
                    stack.push(neighbor);
                }
            }
        }

        // 如果整张图都搜索完了，也没回到起点，说明没有形成环
        return false;

    }

    // 清理 tid 的等待登记（出边/入边 + waitingOn）
    private void clearWaitingOf(TransactionId tid) {
        waitingOn.remove(tid);
        // 删出边
        waitFor.remove(tid);
        // 删入边 (别人指向我的边)
        for (Set<TransactionId> edges : waitFor.values()) {
            edges.remove(tid);
        }
    }

    // 实现锁的获取 acquire
    public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm) throws InterruptedException, TransactionAbortedException {
        // 翻译成锁类型
        LockType want = toLockType(perm);

        // 这页现在的锁长什么样 -> 能否授予锁的规则需要state
        // 从 lockTable 里取，如果没有，就放进去一个“空状态”
        LockState lockState = lockTable.computeIfAbsent(pid, k -> new LockState());

        // 能否授予锁
        // 不能就等待
        while (canGrantLock(lockState, want, tid) == false) {
            // 在wait for graph里登记“我等谁”
            addWaitEdges(tid, lockState, want);
            // 在 waitingOn 里登记“我在等哪页”
            waitingOn.put(tid, pid);
            // 检测是否形成环
            // 形成环了，说明死锁，抛异常
            if (hasCycle(tid)) {

                // 清理等待登记
                clearWaitingOf(tid);
                throw new TransactionAbortedException();
            }
            // 无环，继续等待
            wait();

        }

        // 能授予锁
        if (want == LockType.SHARED) {
            if (lockState.type == null) lockState.type = LockType.SHARED;
            lockState.holders.add(tid);
        } else {
            lockState.type = LockType.EXCLUSIVE;
            lockState.holders.clear();
            lockState.holders.add(tid);
        }

        // 更新映射，这个事务持有哪些页 tid -> pid
        transactionLocks.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);

        // 清理等待登记
        clearWaitingOf(tid);

        // 因为其他等待相同资源（如 S 锁）的事务，现在可能可以被授予锁，需要被唤醒重新判断
        notifyAll();
    }

    public synchronized void release(TransactionId tid, PageId pid) {
        // 这页现在的锁长什么样
        LockState lockState = lockTable.get(pid);

        // 这页没有锁，或者这个事务不持有这页的锁, 直接返回
        if (lockState == null || lockState.holders.contains(tid) == false) {
            return;
        }

        // 释放锁，更新状态
        // 从 holders 里删掉 tid
        lockState.holders.remove(tid);
        // 如果 holders 为空，说明没人持锁了，type 设为 null
        if (lockState.holders.isEmpty()) {
            lockState.type = null;
        }
        // 如果 holders 还剩人 且 当前是共享锁 → 继续保持 SHARED
        // holders 还不空 但 type == EXCLUSIVE（理论上不该发生）→ 设为 SHARED（降级）
        else if (lockState.type == LockType.EXCLUSIVE) {
            lockState.type = LockType.SHARED; // 安全网 + TODO: log/assert
        }
        // else: 保持 SHARED，不用再赋值

        // 更新映射，这个事务持有哪些页 tid -> pid
        // 先拿到该事务当前的页集合
        Set<PageId> pages = transactionLocks.get(tid);

        // 只把这次释放的那一页移除
        if (pages != null) {
            pages.remove(pid);
            // 如果该事务没有持有任何页了，就从映射里删掉这个事务
            if (pages.isEmpty()) transactionLocks.remove(tid);
        }

        for (Set<TransactionId> outs : waitFor.values()) {
            outs.remove(tid); // 清别人等我的入边
        }

        // 唤醒等待的事务
        notifyAll();
    }

    public synchronized void releaseAll(TransactionId tid) {
        // 一次性取出要释放的所有页，并且顺手把这条 tid 的索引删掉
        Set<PageId> holds = transactionLocks.remove(tid);

        // 如果该事务没有持有任何页，直接返回
        if(holds == null || holds.isEmpty()) {
            return;
        }

        // 逐页释放
        for(PageId pid : holds) {
            LockState lockState = lockTable.get(pid);

            // 该页没有状态，跳过
            if (lockState == null) continue;

            lockState.holders.remove(tid);

            // 如果 holders 为空，说明没人持锁了，type 设为 null
            if (lockState.holders.isEmpty()) {
                lockState.type = null;
            }
            else if(lockState.type == LockType.EXCLUSIVE) {
                lockState.type = LockType.SHARED;
            }


        }

        clearWaitingOf(tid);
        // 唤醒等待的事务
        notifyAll();

    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        LockState s = lockTable.get(pid);
        return s != null && s.holders.contains(tid);
    }

}
