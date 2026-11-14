<script setup lang="ts">
import type { HTMLAttributes } from "vue";
import { cn } from "cn";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";

const props = defineProps<{ class?: HTMLAttributes["class"] }>();
</script>

<template>
  <header :class="cn('z-50 flex h-16 shrink-0 items-center gap-3 px-4 sm:gap-4', props.class)">
    <SidebarTrigger class="max-md:scale-125" />
    <Separator orientation="vertical" class="h-6" />
    <slot />
  </header>
</template>
