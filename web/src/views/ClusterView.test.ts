import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { clusterNodes, clusterStatus, removeClusterNode, type ClusterNode } from "../api/cluster";
import { confirmDialog } from "../state/modal";
import ClusterView from "./ClusterView.vue";

vi.mock("../api/cluster", async (original) => ({
  ...(await original<typeof import("../api/cluster")>()),
  clusterNodes: vi.fn(),
  clusterStatus: vi.fn(),
  removeClusterNode: vi.fn(),
}));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
vi.mock("../state/toast", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
const node: ClusterNode = {
  node_id: "node-a",
  journal_id: "journal-a",
  incarnation_id: "instance-a",
  epoch: "9007199254740993",
  revision: "2",
  desired_state: "enabled",
  status: "online",
  rpc_url: "ws://node-a:9802",
  http_url: "http://node-a:9803",
  build: "0.1.0",
  last_seen_at: "2026-10-11T00:00:00Z",
  lease_until: "2026-10-11T00:00:30Z",
  snapshot: {
    durable_lsn: "9007199254740993",
    applied_lsn: "9007199254740992",
    projected_lsn: "9007199254740991",
    master_epoch: "1",
    run_counts: { running: 2, succeeded: 10 },
    writer_error: null,
    projection_error: null,
  },
};
const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(ClusterView);
  wrappers.push(wrapper);
  return wrapper;
}
beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(clusterStatus).mockResolvedValue({
    enabled: true,
    cluster_id: "production",
    node_id: "node-a",
  });
  vi.mocked(clusterNodes).mockResolvedValue({
    cluster_id: "production",
    server_time: "2026-10-11T00:00:00Z",
    nodes: [node],
    next_after: null,
  });
});
afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});
describe("cluster management", () => {
  it("shows disabled mode without querying PG nodes", async () => {
    vi.mocked(clusterStatus).mockResolvedValue({ enabled: false });
    const w = create();
    await flushPromises();
    expect(w.text()).toContain("尚未启用节点注册");
    expect(clusterNodes).not.toHaveBeenCalled();
  });
  it("preserves exact sequence values and keeps last data when refresh fails", async () => {
    const w = create();
    await flushPromises();
    expect(w.text()).toContain("9007199254740993");
    expect(w.text()).toContain("1 / 2");
    vi.mocked(clusterNodes).mockRejectedValueOnce(new Error("PG unavailable"));
    await w.find("header button").trigger("click");
    await flushPromises();
    expect(w.text()).toContain("当前显示上一次成功查询的数据");
    expect(w.text()).toContain("journal-a");
  });
  it("confirms removal and preserves the same command ID across an ambiguous retry", async () => {
    const w = create();
    await flushPromises();
    vi.mocked(removeClusterNode)
      .mockRejectedValueOnce(new Error("response lost"))
      .mockResolvedValueOnce({ removed: true });
    await w.find("button.danger").trigger("click");
    await flushPromises();
    await w.find("button.danger").trigger("click");
    await flushPromises();
    expect(confirmDialog).toHaveBeenCalledWith(
      expect.stringContaining("2 个未终结 run"),
      expect.objectContaining({ danger: true }),
    );
    expect(vi.mocked(removeClusterNode).mock.calls[0]![1]).toEqual(
      vi.mocked(removeClusterNode).mock.calls[1]![1],
    );
    expect(vi.mocked(removeClusterNode).mock.calls[0]![0].epoch).toBe("9007199254740993");
  });
});
