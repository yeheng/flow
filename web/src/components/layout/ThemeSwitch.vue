<script setup lang="ts">
import { Check, Moon, Sun } from "@lucide/vue";
import { cn } from "cn";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { applyTheme, themeChoice, type ThemeChoice } from "./theme";

const options: { value: ThemeChoice; label: string }[] = [
  { value: "light", label: "浅色" },
  { value: "dark", label: "深色" },
  { value: "system", label: "跟随系统" },
];
</script>

<template>
  <DropdownMenu>
    <DropdownMenuTrigger as-child>
      <Button variant="ghost" size="icon" type="button" class="scale-95 rounded-full">
        <Sun class="size-[1.2rem] scale-100 rotate-0 transition-all dark:scale-0 dark:-rotate-90" />
        <Moon
          class="absolute size-[1.2rem] scale-0 rotate-90 transition-all dark:scale-100 dark:rotate-0"
        />
        <span class="sr-only">切换主题</span>
      </Button>
    </DropdownMenuTrigger>
    <DropdownMenuContent align="end">
      <DropdownMenuItem v-for="item in options" :key="item.value" @click="applyTheme(item.value)">
        {{ item.label }}
        <Check :size="14" :class="cn('ms-auto', themeChoice !== item.value && 'hidden')" />
      </DropdownMenuItem>
    </DropdownMenuContent>
  </DropdownMenu>
</template>
