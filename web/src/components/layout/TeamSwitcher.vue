<script setup lang="ts">
import { computed } from "vue";
import { ChevronsUpDown, Command, Plus } from "@lucide/vue";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar } from "@/components/ui/sidebar";
import { environments, selectedEnvironment } from "@/state/environments";

const { isMobile } = useSidebar();

const options = computed(() => [
  { id: "default", name: "默认" },
  ...environments.value.map((item) => ({ id: item.id, name: item.name })),
]);

const current = computed(
  () => options.value.find((item) => item.id === selectedEnvironment.value) ?? options.value[0],
);

function choose(id: string): void {
  selectedEnvironment.value = id;
}
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
            <div
              class="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground"
            >
              <Command class="size-4" />
            </div>
            <div class="grid flex-1 text-start text-sm leading-tight">
              <span class="truncate font-semibold">Flow</span>
              <span class="truncate text-xs">{{ current?.name }}</span>
            </div>
            <ChevronsUpDown class="ms-auto" />
          </SidebarMenuButton>
        </DropdownMenuTrigger>
        <DropdownMenuContent
          class="w-(--reka-dropdown-menu-trigger-width) min-w-56 rounded-lg"
          align="start"
          :side="isMobile ? 'bottom' : 'right'"
          :side-offset="4"
        >
          <DropdownMenuLabel class="text-xs text-muted-foreground">运行环境</DropdownMenuLabel>
          <DropdownMenuItem
            v-for="item in options"
            :key="item.id"
            class="gap-2 p-2"
            @click="choose(item.id)"
          >
            <div class="flex size-6 items-center justify-center rounded-sm border">
              <Command class="size-4 shrink-0" />
            </div>
            {{ item.name }}
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem as-child>
            <RouterLink to="/settings" class="gap-2 p-2">
              <div class="flex size-6 items-center justify-center rounded-md border bg-background">
                <Plus class="size-4" />
              </div>
              <span class="font-medium text-muted-foreground">管理环境</span>
            </RouterLink>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </SidebarMenuItem>
  </SidebarMenu>
</template>
