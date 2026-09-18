# flow-web 前端拖拽流程编辑器

Vue 3 + Vite + TypeScript，画布基于 @vue-flow/core。直连 flow-server
（JSON-RPC 2.0 over WebSocket），覆盖 编辑 → 保存 → 发布 → 运行 → 实时监控 全流程。

## 启动

```bash
# 终端 1：后端（仓库根）
cargo run --bin flow-server          # 默认 ws://127.0.0.1:9800

# 终端 2：前端
cd web
npm install
npm run dev                          # 默认 http://127.0.0.1:5173
```

flow-server 地址可用环境变量覆盖（需在 vite 启动前设置）：

```bash
VITE_FLOW_RPC=ws://127.0.0.1:9801 npm run dev
```

## 使用

1. 左侧「新建」工作流，画布自动放入 start → end 最小定义；
2. 从节点面板拖入或直接点击添加节点（面板按类别分组），拖动手柄连线
   （condition 有 真/假 两个出口，标签标在边上）；
3. 点击节点在右侧编辑参数——表单由 `nodetypes.list` 返回的 JSON Schema
   （`params_schema`，含 x-widget: code/json/workflow-picker）递归渲染；
4. 「保存」（或 Ctrl/Cmd+S）生成 draft 版本（服务端强制 validate），「发布」后可「运行」；
5. 运行时画布按节点状态着色（运行中蓝 / 完成绿 / 失败红 / 重试中黄 / 跳过灰），
   运行中节点的下游边流动动画；右侧时间线实时更新，行 hover/click 与画布节点联动；
   human_task 等待时可交付信号，运行中可取消。
6. sub_workflow 子流程节点：参数面板用下拉选择目标工作流（未发布/不存在有警告），
   双击节点钻取子流程（画布上方面包屑可逐级返回）；运行时节点和时间线上的
   「子 run →」链接可切换到子 run 监控，「← 父 run」返回。

其他画布能力：右下 Minimap、吸附网格（16px）、旧定义缺 position 时按拓扑分层兜底布局；
有未保存修改时切换工作流或钻取会先确认。
