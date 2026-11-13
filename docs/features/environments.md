# 运行环境

顶部「运行环境」决定手动运行、新建定时任务和 Webhook 的环境，以及工作流保存、导入和预检所使用的密钥清单。切换选择不改变已有运行或已有触发器。

在设置页的「运行环境」创建环境：ID 使用字母、数字、下划线或短横线，创建后固定；名称可以修改。`default` 保留给旧配置和全局密钥。普通配置必须是 JSON 对象，例如：

```json
{
  "api_url": "https://example.test",
  "harness": {
    "agent": {
      "base_url": "https://example.test/v1",
      "model": "example-model"
    }
  }
}
```

节点参数使用 `${config.api_url}` 读取普通配置。`harness.agent` 若提供，则作为完整的环境级 agent 配置替换进程默认配置，遵循既有字段校验。密钥必须在密钥栏单独填写，节点继续引用逻辑名称。普通配置会进入运行快照和事件，不能填写秘密值。

每次保存生成新版本。运行在启动时绑定版本；派发、重试、恢复、子流程和回归候选沿用该版本。密钥真值仅存在于加密存储和执行内存，不写入运行快照。新环境不回落到其他环境或全局 `FLOW_SECRET_*`；旧运行和 `default` 保留 stored → env 行为。

「复制配置」只复制普通配置，新的环境 ID 和密钥需要重新填写。删除环境前检查运行历史及未删除的触发器；有引用时拒绝删除。删除当前版本的密钥不会撤销历史版本，历史版本保留以支持重试；当前没有紧急撤销历史密钥版本的管理入口。

进程监听地址、数据目录、执行器部署、证书及服务连接仍由现有统一配置管理，重启生效。运行环境要求工作流使用 semantics 2；semantics 1 继续在默认环境运行。

## RPC

- `environments.list`：返回环境 ID、名称、最新版本、普通配置、密钥名称；不返回密钥值。
- `environments.save`：`{id,name,revision,config,secret?:{name,value}}`；创建时 `revision=0`，更新时传当前版本。`secret.value=null` 删除最新版本中的该密钥。版本冲突拒绝写入。
- `environments.delete`：`{id,revision}`；引用存在或版本冲突时拒绝。
- `run.start`、`workflow.import`、`workflow.update`、`workflow.validate`、`schedule.create`、`webhook.create` 接受可选 `environment_id`，缺省保持旧行为。
- `schedule.change` / `webhook.change` 支持通过 `patch.environment_id` 修改触发器的绑定；不影响已启动的运行。
- `secrets.list` 接受 `environment_id`，返回该环境的名称清单。
- `run.get.view` 的 `environment` 返回实际绑定的 ID、版本和普通配置；旧/default 运行返回 null。

环境存储位于 storage 数据目录下的 `environments/`，复用 AES-GCM 文件存储。备份和恢复需要同时包含该目录及 journal；加密边界与既有密钥存储相同。版本升级为增量添加可选快照字段，旧记录无需迁移。降级到旧二进制不支持新环境语义，应先完成环境运行并保留备份。
