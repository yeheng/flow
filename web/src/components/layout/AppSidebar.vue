<script setup lang="ts">
import { Sidebar, SidebarContent, SidebarFooter, SidebarHeader, SidebarRail } from "@/components/ui/sidebar";
import { sidebarCollapsible, sidebarVariant } from "./layout-state";
import { navGroups } from "./nav";
import NavGroup from "./NavGroup.vue";
import NavUser from "./NavUser.vue";
import TeamSwitcher from "./TeamSwitcher.vue";
</script>

<template>
  <Sidebar :collapsible="sidebarCollapsible" :variant="sidebarVariant">
    <SidebarHeader>
      <TeamSwitcher />
    </SidebarHeader>
    <SidebarContent>
      <nav class="nav">
        <NavGroup v-for="group in navGroups" :key="group.title" :group="group" />
      </nav>
    </SidebarContent>
    <SidebarFooter>
      <NavUser />
    </SidebarFooter>
    <SidebarRail />
  </Sidebar>
</template>
