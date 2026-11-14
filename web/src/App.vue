<script setup lang="ts">
import { computed, watch } from "vue";
import { useRoute } from "vue-router";
import { client } from "./rpc/client";
import AppToasts from "./components/AppToasts.vue";
import AppModal from "./components/AppModal.vue";
import AppSidebar from "./components/layout/AppSidebar.vue";
import CommandMenu from "./components/layout/CommandMenu.vue";
import ConfigDrawer from "./components/layout/ConfigDrawer.vue";
import HeaderBar from "./components/layout/HeaderBar.vue";
import ProfileMenu from "./components/layout/ProfileMenu.vue";
import SearchButton from "./components/layout/SearchButton.vue";
import ThemeSwitch from "./components/layout/ThemeSwitch.vue";
import TopNav from "./components/layout/TopNav.vue";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { environments, refreshEnvironments, selectedEnvironment } from "./state/environments";
import { refreshSecrets } from "./state/editor";

const route = useRoute();

/** 画布页自己有顶栏，不再套一层管理页 header。 */
const instrument = computed(
  () => /^\/workflows\/[^/]+$/.test(route.path) || /^\/runs\/[^/]+$/.test(route.path),
);

watch(
  [selectedEnvironment, () => environments.value.find((e) => e.id === selectedEnvironment.value)?.revision],
  () => {
    void refreshSecrets();
  },
);
watch(
  () => client.connected.value,
  (value) => {
    if (value)
      void refreshEnvironments().catch(() => {
        environments.value = [];
        selectedEnvironment.value = "default";
      });
  },
  { immediate: true },
);
</script>

<template>
  <SidebarProvider class="h-svh overflow-hidden">
    <a class="skip" href="#content">跳到主内容</a>
    <AppSidebar />
    <SidebarInset class="@container/content h-svh min-h-0 overflow-hidden md:h-[calc(100svh-1rem)]">
      <HeaderBar v-if="!instrument">
        <TopNav class="me-auto" />
        <SearchButton />
        <ThemeSwitch />
        <ConfigDrawer />
        <ProfileMenu />
      </HeaderBar>
      <div id="content" class="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        <RouterView />
      </div>
    </SidebarInset>
  </SidebarProvider>
  <CommandMenu />
  <AppToasts />
  <AppModal />
</template>

<style scoped>
.skip {
  position: fixed;
  inset-inline-start: 11rem;
  z-index: 100;
  translate: 0 -13rem;
  background: var(--primary);
  color: var(--primary-foreground);
  padding: 0.5rem 1rem;
  font-size: 0.875rem;
  font-weight: 500;
  border-radius: var(--radius);
}

.skip:focus {
  translate: 0 0.75rem;
}
</style>
