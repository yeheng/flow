<script setup lang="ts">
import { onMounted, onUnmounted } from "vue";
import { useRouter } from "vue-router";
import { ArrowRight, Laptop, Moon, Sun } from "@lucide/vue";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import { navGroups } from "./nav";
import { searchOpen } from "./search-state";
import { applyTheme } from "./theme";

const router = useRouter();

function run(action: () => void): void {
  searchOpen.value = false;
  action();
}

function go(url: string): void {
  run(() => {
    void router.push(url);
  });
}

function onKey(event: KeyboardEvent): void {
  if (event.key.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
    event.preventDefault();
    searchOpen.value = !searchOpen.value;
  }
}

onMounted(() => window.addEventListener("keydown", onKey));
onUnmounted(() => window.removeEventListener("keydown", onKey));
</script>

<template>
  <CommandDialog v-model:open="searchOpen" title="搜索" description="跳到页面或切换主题">
    <CommandInput placeholder="输入页面名称…" />
    <CommandList>
      <CommandEmpty>没有匹配的页面</CommandEmpty>
      <CommandGroup v-for="group in navGroups" :key="group.title" :heading="group.title">
        <template v-for="item in group.items" :key="item.title">
          <CommandItem v-if="item.url" :value="item.title" @select="go(item.url)">
            <ArrowRight class="size-4 text-muted-foreground" />
            {{ item.title }}
          </CommandItem>
          <CommandItem
            v-for="sub in item.items ?? []"
            :key="sub.url"
            :value="`${item.title}-${sub.title}`"
            @select="go(sub.url)"
          >
            <ArrowRight class="size-4 text-muted-foreground" />
            {{ item.title }} / {{ sub.title }}
          </CommandItem>
        </template>
      </CommandGroup>
      <CommandSeparator />
      <CommandGroup heading="主题">
        <CommandItem value="浅色" @select="run(() => applyTheme('light'))">
          <Sun />
          浅色
        </CommandItem>
        <CommandItem value="深色" @select="run(() => applyTheme('dark'))">
          <Moon />
          深色
        </CommandItem>
        <CommandItem value="跟随系统" @select="run(() => applyTheme('system'))">
          <Laptop />
          跟随系统
        </CommandItem>
      </CommandGroup>
    </CommandList>
  </CommandDialog>
</template>
