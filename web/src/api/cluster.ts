import { client } from "../rpc/client";

export type ClusterStatus =
  { enabled: false } | { enabled: true; cluster_id: string; node_id: string };
export interface ClusterNode {
  node_id: string;
  journal_id: string;
  incarnation_id: string;
  epoch: string;
  revision: string;
  desired_state: "enabled" | "removed";
  status: "online" | "degraded" | "offline" | "stopped" | "removed";
  rpc_url: string;
  http_url: string;
  build: string;
  last_seen_at: string;
  lease_until: string;
  snapshot: {
    durable_lsn: string;
    applied_lsn: string;
    projected_lsn: string;
    master_epoch: string;
    run_counts: Record<string, number>;
    writer_error: string | null;
    projection_error: string | null;
  };
}
export interface ClusterPage {
  cluster_id: string;
  server_time: string;
  nodes: ClusterNode[];
  next_after: string | null;
}
export function clusterStatus(): Promise<ClusterStatus> {
  return client.call("journal.cluster.status", {});
}
export function clusterNodes(after = ""): Promise<ClusterPage> {
  return client.call("journal.cluster.nodes", { after, limit: 64 });
}
export function removeClusterNode(node: ClusterNode, requestId: string): Promise<unknown> {
  return client.call("journal.cluster.remove", {
    node_id: node.node_id,
    expected_epoch: node.epoch,
    request_id: requestId,
  });
}
/** Sequence values must stay exact beyond Number.MAX_SAFE_INTEGER. */
export function progressLag(durable: string, cursor: string): string {
  return (BigInt(durable) - BigInt(cursor)).toString();
}
