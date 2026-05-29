# 命名规约
> 此文件用于命名规约，约定如下，不可违反约束

## 1.新三段式
> 将原来Namespace/ServiceGroup/Endpoint更改为贴近k8s原生三段式服务
- 1.Namespace
- 2.ServiceGroup
- 3.PortName

## 2.pagoda前缀
- 1.所有涉及dynamo前缀命名的常量、变量、字符串、函数名等均改为pagoda,注意大小写区分

## 3.服务发现模型实例
- 1.模型实例中新增字段:topo_json: serde_json::Value

## 4.timeline事件线标注
- 1.nvtx模块更改为timeline模块
- 2.其中涉及的四个宏前缀名字更改为pagoda_timeline_