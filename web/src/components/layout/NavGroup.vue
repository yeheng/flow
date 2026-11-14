<script setup lang="ts">
import { ChevronRight } from "@lucide/vue";
import { useRoute } from "vue-router";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SidebarGroup,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { navActive, type NavGroupData, type NavItem } from "./nav";

defineProps<{ group: NavGroupData }>();

const route = useRoute();
const { state, isMobile, setOpenMobile } = useSidebar();

function active(item: NavItem, main = false): boolean {
  if (item.url && navActive(route.path, item.url)) return true;
  if (item.items?.some((child) => navActive(route.path, child.url))) return true;
  if (main && item.url) {
    const section = route.path.split("/")[1] ?? "";
    const itemSection = item.url.split("/")[1] ?? "";
    return section !== "" && section === itemSection;
  }
  return false;
}

function closeMobile(): void {
  setOpenMobile(false);
}
</script>

<template>
  <SidebarGroup>
    <SidebarGroupLabel>{{ group.title }}</SidebarGroupLabel>
    <SidebarMenu>
      <template v-for="item in group.items" :key="item.title">
        <SidebarMenuItem v-if="!item.items">
          <SidebarMenuButton as-child :is-active="active(item)" :tooltip="item.title">
            <RouterLink :to="item.url!" :class="{ 'nav-active': active(item) }" @click="closeMobile">
              <component :is="item.icon" v-if="item.icon" />
              <span>{{ item.title }}</span>
              <Badge v-if="item.badge" class="rounded-full px-1 py-0 text-xs">{{ item.badge }}</Badge>
            </RouterLink>
          </SidebarMenuButton>
        </SidebarMenuItem>

        <SidebarMenuItem v-else-if="state === 'collapsed' && !isMobile">
          <DropdownMenu>
            <DropdownMenuTrigger as-child>
              <SidebarMenuButton :tooltip="item.title" :is-active="active(item)">
                <component :is="item.icon" v-if="item.icon" />
                <span>{{ item.title }}</span>
                <Badge v-if="item.badge" class="rounded-full px-1 py-0 text-xs">{{ item.badge }}</Badge>
                <ChevronRight class="ms-auto transition-transform duration-200" />
              </SidebarMenuButton>
            </DropdownMenuTrigger>
            <DropdownMenuContent side="right" align="start" :side-offset="4">
              <DropdownMenuLabel>
                {{ item.title }}{{ item.badge ? ` (${item.badge})` : "" }}
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem v-for="sub in item.items" :key="sub.url" as-child>
                <RouterLink :to="sub.url" :class="active(sub) ? 'bg-secondary' : ''" @click="closeMobile">
                  <component :is="sub.icon" v-if="sub.icon" />
                  <span class="max-w-52 text-wrap">{{ sub.title }}</span>
                </RouterLink>
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </SidebarMenuItem>

        <Collapsible v-else :default-open="active(item, true)" class="group/collapsible">
          <SidebarMenuItem>
            <CollapsibleTrigger as-child>
              <SidebarMenuButton :tooltip="item.title">
                <component :is="item.icon" v-if="item.icon" />
                <span>{{ item.title }}</span>
                <Badge v-if="item.badge" class="rounded-full px-1 py-0 text-xs">{{ item.badge }}</Badge>
                <ChevronRight
                  class="ms-auto transition-transform duration-200 group-data-[state=open]/collapsible:rotate-90"
                />
              </SidebarMenuButton>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <SidebarMenuSub>
                <SidebarMenuSubItem v-for="sub in item.items" :key="sub.url">
                  <SidebarMenuSubButton as-child :is-active="active(sub)">
                    <RouterLink :to="sub.url" @click="closeMobile">
                      <component :is="sub.icon" v-if="sub.icon" />
                      <span>{{ sub.title }}</span>
                    </RouterLink>
                  </SidebarMenuSubButton>
                </SidebarMenuSubItem>
              </SidebarMenuSub>
            </CollapsibleContent>
          </SidebarMenuItem>
        </Collapsible>
      </template>
    </SidebarMenu>
  </SidebarGroup>
</template>
