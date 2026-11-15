# 常驻 Echo 插件样例

先构建 `cargo build --offline -p flow-plugin-sdk --example resident_echo`，再将本目录复制到 Flow 的 plugins/resident-echo，构建产物放在 bin/resident_echo。

也可以先在本目录的 `bin/resident_echo` 放好构建产物，再通过设置页安装，或调用 `flow journal plugins install --source /absolute/path/to/examples/resident-echo`。

在「设置 → 插件服务」启动 resident-echo，打开界面发送消息。关闭界面后服务仍然运行，停止服务会回收进程并撤销页面地址。

完整协议、权限与边界见 [常驻插件说明](../../docs/features/resident-plugin-services.md)。示例默认只开放 echo；测试环境额外开放 relay/host 并声明 peer，用于验证宿主路由与节点绑定。
