<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "cn";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <header :class="cn('z-50 h-16 shrink-0', props.class)">
    <div class="relative flex h-full items-center gap-3 p-4 sm:gap-4">
      <SidebarTrigger />
      <Separator orientation="vertical" class="h-6" />
      <slot />
    </div>
  </header>
</template>
