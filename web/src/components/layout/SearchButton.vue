<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { Search } from "@lucide/vue";
import { cn } from "cn";
import { Button } from "@/components/ui/button";
import { searchOpen } from "./search-state";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();

function open(): void {
  searchOpen.value = true;
}
</script>

<template>
  <Button
    variant="outline"
    type="button"
    aria-keyshortcuts="Meta+K Control+K"
    :class="
      cn(
        'group relative h-8 w-full flex-1 justify-start rounded-md bg-muted/25 text-sm font-normal text-muted-foreground shadow-none hover:bg-accent sm:w-40 sm:pe-12 md:flex-none lg:w-52 xl:w-64',
        props.class,
      )
    "
    @click="open"
  >
    <Search aria-hidden="true" class="absolute inset-s-1.5 top-1/2 -translate-y-1/2" :size="16" />
    <span class="ms-4">搜索</span>
    <kbd
      class="pointer-events-none absolute inset-e-[0.3rem] top-[0.3rem] hidden h-5 items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium opacity-100 select-none group-hover:bg-accent sm:flex"
    >
      <span class="text-xs">⌘</span>K
    </kbd>
  </Button>
</template>
