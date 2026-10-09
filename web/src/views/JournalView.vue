<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef, triggerRef } from "vue";
import { EventWindow, JournalClient, type JournalPage, type Receipt, type RunSnapshot } from "../api/journal";
import { isDesktop } from "../platform";
import { confirmDialog } from "../state/modal";
const desktop = isDesktop();
const url=ref("ws://127.0.0.1:9802"), http=ref("http://127.0.0.1:9803"), token=ref("");
const api=shallowRef<JournalClient>();
const workflows=ref<Array<{workflow_id:string;name:string}>>([]),runs=ref<RunSnapshot[]>([]);
const workflow=ref(""),name=ref(""),input=ref("{}"),definition=ref('{"nodes":[{"id":"start","type":"start"},{"id":"end","type":"end"}],"edges":[{"from":"start","to":"end"}]}');
const run=shallowRef<RunSnapshot>(),page=shallowRef<JournalPage>(),audit=ref(false),message=ref(""),busy=ref(false);
const windowed=shallowRef(new EventWindow()),observations=shallowRef<unknown[]>([]),observationLoss=shallowRef<unknown>();
let refreshing=false, frame:number|undefined;
let stop:(()=>Promise<void>)|undefined,timer:ReturnType<typeof setInterval>|undefined;
const detail=computed(()=>JSON.stringify(run.value,null,2));
const downloads=computed(()=>{
  const refs=new Set<string>();
  const visit=(v:unknown):void=>{if(!v || typeof v!=="object")return;const o=v as Record<string,unknown>;if(o.type==="ref" && o.value && typeof o.value==="object"){const id=(o.value as Record<string,unknown>).output_id;if(typeof id==="string")refs.add(id);}for(const child of Object.values(o))visit(child);};
  visit(run.value);return [...refs];
});
async function act(work:()=>Promise<void>){busy.value=true;message.value="";try{await work();}catch(e){message.value=String(e);}finally{busy.value=false;}}
function receipt(r:Receipt){message.value=r.visible?"已提交并可见":`已提交，投影尚未可见。请求 ${r.request_id}，提交 ${r.commit_cursor.lsn}；请查询原请求，勿重复创建。`;}
async function load(){if(!api.value)return;workflows.value=(await api.value.call<{values:typeof workflows.value}>("workflow.list",{limit:64})).values;runs.value=(await api.value.call<{values:RunSnapshot[]}>("run.list",{limit:64})).values;}
async function connect(){api.value?.close();await stop?.();stop=undefined;if(timer)clearInterval(timer);run.value=undefined;api.value=new JournalClient(url.value,token.value,http.value);await load();}
async function create(){if(!api.value)return;const r=await api.value.command("workflow.create",{name:name.value});receipt(r);workflow.value=String(r.result.workflow_id);await load();}
async function save(){if(!api.value)return;if(definition.value.length>1024*1024)throw new Error("定义超过 1 MiB");const r=await api.value.command("workflow.update",{workflow_id:workflow.value,definition:JSON.parse(definition.value)});receipt(r);if(!r.visible)return;const published=await api.value.command("workflow.publish",{workflow_id:workflow.value,version:r.result.version});receipt(published);}
async function start(){if(!api.value)return;const r=await api.value.command("run.start",{workflow_id:workflow.value,input:JSON.parse(input.value)},"run.start:manual:");receipt(r);if(r.visible)await watchRun(String(r.result.run_id));await load();}
async function refreshRun(){if(!api.value || !run.value || refreshing)return;refreshing=true;try{const id=run.value.run_id;const current=await api.value.call<{value:RunSnapshot}>("run.get",{run_id:id});if(run.value?.run_id!==id)return;run.value=current.value;const logs=await api.value.call<{records:unknown[];loss:unknown}>("run.observations.page",{run_id:id,limit:100});if(run.value?.run_id!==id)return;observations.value=logs.records;observationLoss.value=logs.loss;}finally{refreshing=false;}}
async function watchRun(id:string){await stop?.();if(timer)clearInterval(timer);windowed.value=new EventWindow();page.value=undefined;stop=await api.value!.monitor(id,v=>{run.value=v;},e=>{windowed.value.push(e);if(frame===undefined)frame=requestAnimationFrame(()=>{frame=undefined;triggerRef(windowed);});},n=>{if(n)message.value=`实时缓冲丢弃 ${n} 条，已按快照重新对齐；历史可分页读取。`;});timer=setInterval(()=>{void refreshRun().catch(e=>{message.value=String(e);});},2000);await refreshRun();}
async function nextPage(reset=false){if(!api.value || !run.value)return;page.value=await api.value.page(run.value.run_id,audit.value,reset?undefined:page.value?.next_cursor);}
async function cancel(){if(!api.value || !run.value)return;if(!(await confirmDialog(`确定取消 run ${run.value.run_id.slice(0,8)}…？`,{danger:true})))return;receipt(await api.value.command("run.cancel",{run_id:run.value.run_id},`run.cancel:${run.value.run_id}`));await refreshRun();}
onBeforeUnmount(()=>{api.value?.close();if(frame!==undefined)cancelAnimationFrame(frame);void stop?.();if(timer)clearInterval(timer);});
</script>

<template>
  <main class="journal">
    <h1>JSONL 工作区</h1>
    <form class="bar" @submit.prevent="act(connect)">
      <template v-if="!desktop">
        <label>RPC 地址 <input v-model="url" required /></label><label>下载地址 <input v-model="http" required /></label>
        <label>访问令牌 <input v-model="token" type="password" autocomplete="off" required /></label>
      </template>
      <button :disabled="busy">{{ desktop ? "打开本地工作区" : "连接" }}</button>
    </form>
    <p role="status">{{ message }}</p>
    <template v-if="api">
      <section><h2>工作流</h2><div class="bar"><select v-model="workflow"><option value="">选择工作流</option><option v-for="w in workflows" :key="w.workflow_id" :value="w.workflow_id">{{ w.name }}</option></select><input v-model="name" placeholder="新工作流名称" /><button :disabled="busy || !name" @click="act(create)">新建</button></div>
        <label>定义 JSON<textarea maxlength="1048576" v-model="definition" rows="6" spellcheck="false" /></label><button :disabled="busy || !workflow" @click="act(save)">保存并发布新版本</button>
        <label>运行输入 JSON<textarea maxlength="8388608" v-model="input" rows="3" spellcheck="false" /></label><button :disabled="busy || !workflow" @click="act(start)">开始运行</button>
      </section>
      <section><h2>运行记录</h2><button :disabled="busy" @click="act(load)">刷新</button><p>每次最多展示 64 条。完整历史可通过分页接口读取。</p><div class="runs"><button v-for="r in runs" :key="r.run_id" @click="act(()=>watchRun(r.run_id))">{{ r.run_id }} · {{ r.status }}</button></div></section>
      <section v-if="run"><h2>运行 {{ run.run_id }} · {{ run.status }}</h2><button :disabled="busy" @click="act(cancel)">取消运行</button><pre>{{ detail }}</pre>
        <div class="bar"><button v-for="id in downloads" :key="id" @click="act(()=>api!.download(run!.run_id,id))">下载完整值 {{ id }}</button></div>
        <h3>实时事件（展示丢弃 {{ windowed.dropped }} 条）</h3><pre>{{ JSON.stringify(windowed.events,null,2) }}</pre>
        <h3>历史事件与审计</h3><label><input v-model="audit" type="checkbox" @change="page=undefined" /> 审计记录</label><button @click="act(()=>nextPage(true))">读取新快照</button><button :disabled="!page?.next_cursor" @click="act(()=>nextPage())">下一页</button><pre>{{ JSON.stringify(page?.events ?? [],null,2) }}</pre>
        <h3>观测日志</h3><p>以下丢弃计数属于整个观测存储，不代表业务数据丢失。</p><pre>{{ JSON.stringify(observationLoss) }}</pre><pre>{{ JSON.stringify(observations,null,2) }}</pre>
      </section>
    </template>
  </main>
</template>
<style scoped>
.journal{padding:24px;overflow:auto;max-width:1200px;width:100%;margin:auto}.bar{display:flex;flex-wrap:wrap;gap:12px;align-items:end}section{padding:20px;margin:16px 0;background:var(--surface);border:1px solid var(--border);border-radius:8px}label{display:block;margin:8px 0}input,select,textarea{background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px}textarea{display:block;width:100%;font-family:var(--mono)}button{padding:8px 12px;cursor:pointer}button:disabled{opacity:.5;cursor:default}.runs{display:flex;flex-direction:column;gap:6px}pre{max-height:360px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;background:var(--bg);padding:12px;font-size:12px}
</style>
