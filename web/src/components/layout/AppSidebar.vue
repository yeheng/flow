<script setup lang="ts">
import { useRoute } from "vue-router";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  useSidebar,
} from "@/components/ui/sidebar";
import { navActive, navGroups } from "./nav";
import NavUser from "./NavUser.vue";
import TeamSwitcher from "./TeamSwitcher.vue";

const route = useRoute();
const { setOpenMobile } = useSidebar();

function closeMobile(): void {
  setOpenMobile(false);
}
</script>

<template>
  <Sidebar collapsible="icon" variant="inset">
    <SidebarHeader>
      <TeamSwitcher />
    </SidebarHeader>
    <SidebarContent>
      <nav class="nav">
        <SidebarGroup v-for="group in navGroups" :key="group.title">
          <SidebarGroupLabel>{{ group.title }}</SidebarGroupLabel>
          <SidebarMenu>
            <SidebarMenuItem v-for="item in group.items" :key="item.url">
              <SidebarMenuButton
                as-child
                :is-active="navActive(route.path, item.url)"
                :tooltip="item.title"
              >
                <RouterLink
                  :to="item.url"
                  :class="{ 'nav-active': navActive(route.path, item.url) }"
                  @click="closeMobile"
                >
                  <component :is="item.icon" />
                  <span>{{ item.title }}</span>
                </RouterLink>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroup>
      </nav>
    </SidebarContent>
    <SidebarFooter>
      <NavUser />
    </SidebarFooter>
    <SidebarRail />
  </Sidebar>
</template>
