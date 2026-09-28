import { createRouter, createWebHistory } from "vue-router";

// 路由级代码分割：各视图按需加载，首屏不背编辑器/画布的成本
export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", component: () => import("./views/DashboardView.vue") },
    { path: "/workflows", component: () => import("./views/WorkflowListView.vue") },
    { path: "/workflows/:id", component: () => import("./views/EditorView.vue") },
    {
      path: "/workflows/:id/versions",
      component: () => import("./views/VersionsView.vue"),
      props: (r) => ({ workflowId: r.params.id as string }),
    },
    {
      path: "/workflows/:id/triggers",
      component: () => import("./views/TriggersView.vue"),
      props: (r) => ({ workflowId: r.params.id as string }),
    },
    {
      path: "/workflows/:id/runs",
      component: () => import("./views/RunListView.vue"),
      props: (r) => ({ workflowId: r.params.id as string }),
    },
    { path: "/runs", component: () => import("./views/RunListView.vue") },
    {
      path: "/runs/:runId",
      component: () => import("./views/RunDetailView.vue"),
      props: true,
    },
  ],
});
