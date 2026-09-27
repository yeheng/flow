import { createRouter, createWebHistory } from "vue-router";
import DashboardView from "./views/DashboardView.vue";
import WorkflowListView from "./views/WorkflowListView.vue";
import EditorView from "./views/EditorView.vue";
import RunListView from "./views/RunListView.vue";
import RunDetailView from "./views/RunDetailView.vue";
import VersionsView from "./views/VersionsView.vue";
import TriggersView from "./views/TriggersView.vue";

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", component: DashboardView },
    { path: "/workflows", component: WorkflowListView },
    { path: "/workflows/:id", component: EditorView },
    {
      path: "/workflows/:id/versions",
      component: VersionsView,
      props: (r) => ({ workflowId: r.params.id as string }),
    },
    {
      path: "/workflows/:id/triggers",
      component: TriggersView,
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
