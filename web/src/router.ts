import { createRouter, createWebHistory } from "vue-router";
import WorkflowListView from "./views/WorkflowListView.vue";
import EditorView from "./views/EditorView.vue";
import RunListView from "./views/RunListView.vue";
import RunDetailView from "./views/RunDetailView.vue";
import VersionsView from "./views/VersionsView.vue";

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", redirect: "/workflows" },
    { path: "/workflows", component: WorkflowListView },
    { path: "/workflows/:id", component: EditorView },
    {
      path: "/workflows/:id/versions",
      component: VersionsView,
      props: (r) => ({ workflowId: r.params.id as string }),
    },
    {
      path: "/workflows/:id/runs",
      component: RunListView,
      props: (r) => ({ workflowId: r.params.id as string }),
    },
    { path: "/runs", component: RunListView },
    { path: "/runs/:runId", component: RunDetailView, props: true },
  ],
});
