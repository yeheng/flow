<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { Menu } from "@lucide/vue";
import { useRoute } from "vue-router";
import { cn } from "cn";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { navActive, navGroups } from "./nav";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
const route = useRoute();

const links = navGroups.flatMap((group) =>
  group.items.flatMap((item) =>
    item.url ? [{ title: item.title, href: item.url }] : (item.items ?? []).map((sub) => ({
      title: sub.title,
      href: sub.url,
    })),
  ),
);
</script>

<template>
  <DropdownMenu>
    <DropdownMenuTrigger as-child>
      <Button
        size="icon"
        variant="outline"
        type="button"
        :class="cn('md:size-7 lg:hidden', props.class)"
      >
        <Menu />
        <span class="sr-only">打开页面菜单</span>
      </Button>
    </DropdownMenuTrigger>
    <DropdownMenuContent side="bottom" align="start">
      <DropdownMenuItem v-for="link in links" :key="link.href" as-child>
        <RouterLink :to="link.href" :class="navActive(route.path, link.href) ? '' : 'text-muted-foreground'">
          {{ link.title }}
        </RouterLink>
      </DropdownMenuItem>
    </DropdownMenuContent>
  </DropdownMenu>

  <nav :class="cn('hidden items-center space-x-4 lg:flex xl:space-x-6', props.class)">
    <RouterLink
      v-for="link in links"
      :key="link.href"
      :to="link.href"
      class="text-sm font-medium transition-colors hover:text-primary"
      :class="navActive(route.path, link.href) ? '' : 'text-muted-foreground'"
    >
      {{ link.title }}
    </RouterLink>
  </nav>
</template>
