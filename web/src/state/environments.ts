import { ref } from "vue";
import { client } from "../rpc/client";
export interface Environment {
  id: string;
  name: string;
  revision: number;
  config: Record<string, unknown>;
  secrets: string[];
}
export const environments = ref<Environment[]>([]);
export const selectedEnvironment = ref("default");
export async function refreshEnvironments(): Promise<void> {
  const result = await client.call<{ environments: Environment[] }>("environments.list", {});
  environments.value = result.environments;
}
export async function saveEnvironment(params: Record<string, unknown>): Promise<void> {
  await client.call("environments.save", params);
  await refreshEnvironments();
}
