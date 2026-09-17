import { client } from "../rpc/client";
import type {
  Definition,
  NodeTypeDesc,
  RunEvent,
  Timeline,
  WorkflowDetail,
  WorkflowSummary,
} from "../types";

export async function listNodeTypes(): Promise<NodeTypeDesc[]> {
  const r = await client.call<{ node_types: NodeTypeDesc[] }>("nodetypes.list", {});
  return r.node_types;
}

export async function listWorkflows(): Promise<WorkflowSummary[]> {
  const r = await client.call<{ workflows: WorkflowSummary[] }>("workflow.list", {});
  return r.workflows;
}

export async function createWorkflow(name: string): Promise<string> {
  const r = await client.call<{ workflow_id: string }>("workflow.create", { name });
  return r.workflow_id;
}

export async function updateWorkflow(
  workflowId: string,
  definition: Definition,
): Promise<number> {
  const r = await client.call<{ workflow_id: string; version: number }>("workflow.update", {
    workflow_id: workflowId,
    definition: definition as unknown as Record<string, unknown>,
  });
  return r.version;
}

export async function publishWorkflow(workflowId: string, version: number): Promise<void> {
  await client.call("workflow.publish", { workflow_id: workflowId, version });
}

export async function getWorkflow(workflowId: string): Promise<WorkflowDetail> {
  return client.call<WorkflowDetail>("workflow.get", { workflow_id: workflowId });
}

export async function deleteWorkflow(workflowId: string): Promise<void> {
  await client.call("workflow.delete", { workflow_id: workflowId });
}

export async function startRun(
  workflowId: string,
  input?: unknown,
): Promise<{ run_id: string; workflow_version: number }> {
  const params: Record<string, unknown> = { workflow_id: workflowId };
  if (input !== undefined) params.input = input;
  return client.call("run.start", params);
}

export async function runTimeline(runId: string): Promise<Timeline> {
  return client.call<Timeline>("run.timeline", { run_id: runId });
}

export async function runEvents(runId: string, fromSeq?: number): Promise<RunEvent[]> {
  const params: Record<string, unknown> = { run_id: runId };
  if (fromSeq !== undefined) params.from_seq = fromSeq;
  const r = await client.call<{ events: RunEvent[] }>("run.events", params);
  return r.events;
}

export async function runCancel(runId: string): Promise<void> {
  await client.call("run.cancel", { run_id: runId });
}

export async function runSignal(runId: string, nodeId: string, payload: unknown): Promise<void> {
  await client.call("run.signal", { run_id: runId, node_id: nodeId, payload });
}

export function subscribeRun(
  runId: string,
  onEvent: (e: RunEvent) => void,
): Promise<() => Promise<void>> {
  return client.subscribe("run.subscribe", { run_id: runId }, (e) => onEvent(e as RunEvent));
}
