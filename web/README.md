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
2. 从节点面板拖入节点，拖动手柄连线（condition 有 真/假 两个出口）；
3. 点击节点在右侧编辑参数（schema 来自 `nodetypes.list`）；
4. 「保存」生成 draft 版本（服务端强制 validate），「发布」后可「运行」；
5. 运行时画布按节点状态着色（运行中蓝 / 完成绿 / 失败红 / 重试中黄 / 跳过灰），
   右侧时间线实时更新；human_task 等待时可交付信号，运行中可取消。
