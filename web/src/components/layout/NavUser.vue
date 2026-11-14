<script setup lang="ts">
import { computed } from "vue";
import { BadgeCheck, ChevronsUpDown } from "@lucide/vue";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar } from "@/components/ui/sidebar";
import { client } from "@/rpc/client";

const { isMobile } = useSidebar();
const connected = computed(() => client.connected.value);
const status = computed(() => (connected.value ? "已连接" : "未连接"));
</script>

<template>
  <SidebarMenu>
    <SidebarMenuItem>
      <DropdownMenu>
        <DropdownMenuTrigger as-child>
          <SidebarMenuButton
            size="lg"
            class="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
          >
            <Avatar class="h-8 w-8 rounded-lg">
              <AvatarFallback class="rounded-lg">F</AvatarFallback>
            </Avatar>
            <div class="grid flex-1 text-start text-sm leading-tight">
              <span class="truncate font-semibold">Flow</span>
              <span class="conn truncate text-xs" :class="{ ok: connected }" :title="client.url">
                <span class="conn-dot" aria-hidden="true" />
                {{ status }}
              </span>
            </div>
            <ChevronsUpDown class="ms-auto size-4" />
          </SidebarMenuButton>
        </DropdownMenuTrigger>
        <DropdownMenuContent
          class="w-(--reka-dropdown-menu-trigger-width) min-w-56 rounded-lg"
          :side="isMobile ? 'bottom' : 'right'"
          align="end"
          :side-offset="4"
        >
          <DropdownMenuLabel class="p-0 font-normal">
            <div class="flex items-center gap-2 px-1 py-1.5 text-start text-sm">
              <Avatar class="h-8 w-8 rounded-lg">
                <AvatarFallback class="rounded-lg">F</AvatarFallback>
              </Avatar>
              <div class="grid flex-1 text-start text-sm leading-tight">
                <span class="truncate font-semibold">Flow</span>
                <span class="truncate text-xs text-muted-foreground">{{ status }}</span>
              </div>
            </div>
          </DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem as-child>
            <RouterLink to="/settings">
              <BadgeCheck />
              设置
            </RouterLink>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </SidebarMenuItem>
  </SidebarMenu>
</template>

<style scoped>
.conn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--destructive);
}

.conn.ok {
  color: var(--ok);
}

.conn-dot {
  width: 7px;
  height: 7px;
  border-radius: 999px;
  background: currentColor;
}
</style>
