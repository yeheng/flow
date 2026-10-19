/**
 * 过期作废（generation guard）的唯一实现。
 * 异步响应落地前必须比对代际：begin() 开启一次操作并取得令牌；
 * invalidate() 让所有在飞令牌作废（状态切换/重置）；stale() 判令牌是否过期。
 * 之前 RunDetailView/RunListView/monitor/editor/JournalClient 各抄一份，
 * 漏一份就出竞态（见 RunListView 筛选残留 P0）——从此只此一份。
 */
export interface StaleGuard {
  /** 开启新操作：返回令牌；更晚的 begin()/invalidate() 会让它过期 */
  begin(): number;
  /** 记录当前代际但不作废他人：给补拉/增量这类「观察型」异步用 */
  mark(): number;
  /** 作废所有在飞操作（切换状态/重置时调用） */
  invalidate(): void;
  /** 令牌是否已过期：响应落地前必须比对，过期则丢弃写回 */
  stale(token: number): boolean;
}

export function staleGuard(): StaleGuard {
  let current = 0;
  return {
    begin: () => ++current,
    mark: () => current,
    invalidate: () => {
      current++;
    },
    stale: (token: number) => token !== current,
  };
}
