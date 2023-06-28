package org.apache.griffin.core.worker.service;

import org.apache.griffin.api.entity.enums.DQInstanceStatus;
import org.apache.griffin.core.common.utils.context.WorkerContext;
import org.apache.griffin.core.worker.entity.bo.DQInstanceBO;
import org.apache.griffin.core.worker.schedule.TaskDispatcherScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WorkCoreService {

    @Autowired
    private WorkerContext workerContext;

    @Autowired
    private DQWorkerInstanceService dqWorkerInstanceService;

    @Autowired
    private TaskDispatcherScheduler taskDispatcherScheduler;

    public void submitDQTask(Long instanceId) {
        // 收到提交请求 参数校验
        if (instanceId == null) return;
        // 已存在的instanceId 失败
        if (dqWorkerInstanceService.isRepeatedInstanceId(instanceId)) return;
        // 判断环境是否可以接收任务
        if (!workerContext.canSubmitToQueue()) return;
        // 构建任务实例 怎么保证实例生成的ID一样？ 1 实例ID由master指派  2 entity id + partition 3 实例由master生成
        DQInstanceBO dqInstanceBO = dqWorkerInstanceService.getById(instanceId);
        // 根据状态提交队列等待执行
        workerContext.offerToSpecQueueByStatus(dqInstanceBO);
    }

    public void stopDQTask(Long instanceId) {
        if (instanceId == null) return;
        DQInstanceBO dqInstanceBO = workerContext.getById(instanceId);
        dqWorkerInstanceService.updateStatus(dqInstanceBO, DQInstanceStatus.STOPPED);
    }

    public void querySingleDQTask(Long instanceId) {

    }

}
