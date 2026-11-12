# 记录器历史接口

/boots 返回 bootId 清单及单次记录上限。/anchors 返回 revision 和校时锚点，utc_us 为相对于 2026-11-12T00:00:00Z 的整数微秒，tick 为启动期内整数计数。给定相邻两个锚点，闭区间内用精确有理数线性映射，超出区间不得确认。不同 bootId 可以覆盖相同业务时间，不能互相去重。

/samples?boot=A&from=0&until=100 返回含端点的 tick 范围，按 tick 排序，最多三条并以 truncated=true 明示被截断。from/until 可任意整数，查询空区间会返回空数组与 truncated=false，不存在 boot 返回 404。记录身份为 bootId+tick，value 是十进制文本，raw revision 独立于 clock revision。

初始只一个锚点；/control/phase?value=complete 形成有界映射；value=revised 修改已有映射；value=conflict 让 A:20 有两条不同值；value=unavailable 让样本接口 503。建议演练业务窗口 [60秒,160秒]，revision 从 clock2 到 clock3 时该窗口对应的 tick 范围不同。合成样本 A 每10 tick一条，B 每20 tick一条，设备重启前后 tick 都从零开始。
