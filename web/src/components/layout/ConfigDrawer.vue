<script setup lang="ts">
import type { Component } from "vue";
import { CircleCheck, RotateCcw, Settings } from "@lucide/vue";
import { cn } from "cn";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { useSidebar } from "@/components/ui/sidebar";
import IconDir from "./icons/IconDir.vue";
import IconLayoutCompact from "./icons/IconLayoutCompact.vue";
import IconLayoutDefault from "./icons/IconLayoutDefault.vue";
import IconLayoutFull from "./icons/IconLayoutFull.vue";
import IconSidebarFloating from "./icons/IconSidebarFloating.vue";
import IconSidebarInset from "./icons/IconSidebarInset.vue";
import IconSidebarSidebar from "./icons/IconSidebarSidebar.vue";
import IconThemeDark from "./icons/IconThemeDark.vue";
import IconThemeLight from "./icons/IconThemeLight.vue";
import IconThemeSystem from "./icons/IconThemeSystem.vue";
import {
  direction,
  resetLayout,
  setDirection,
  setSidebarCollapsible,
  setSidebarVariant,
  sidebarCollapsible,
  sidebarVariant,
  type Direction,
  type SidebarCollapsible,
  type SidebarVariant,
} from "./layout-state";
import { applyTheme, themeChoice, type ThemeChoice } from "./theme";

const { open, setOpen } = useSidebar();

const themes: { value: ThemeChoice; label: string; icon: Component }[] = [
  { value: "system", label: "跟随系统", icon: IconThemeSystem },
  { value: "light", label: "浅色", icon: IconThemeLight },
  { value: "dark", label: "深色", icon: IconThemeDark },
];

const variants: { value: SidebarVariant; label: string; icon: Component }[] = [
  { value: "inset", label: "内嵌", icon: IconSidebarInset },
  { value: "floating", label: "浮动", icon: IconSidebarFloating },
  { value: "sidebar", label: "固定", icon: IconSidebarSidebar },
];

const layouts: { value: "default" | SidebarCollapsible; label: string; icon: Component }[] = [
  { value: "default", label: "默认", icon: IconLayoutDefault },
  { value: "icon", label: "紧凑", icon: IconLayoutCompact },
  { value: "offcanvas", label: "收起", icon: IconLayoutFull },
];

const directions: { value: Direction; label: string }[] = [
  { value: "ltr", label: "从左到右" },
  { value: "rtl", label: "从右到左" },
];

const layoutValue = () => (open.value ? "default" : sidebarCollapsible.value);

function chooseLayout(value: string): void {
  if (value === "default") {
    setOpen(true);
    return;
  }
  setOpen(false);
  setSidebarCollapsible(value as SidebarCollapsible);
}

function resetAll(): void {
  setOpen(true);
  applyTheme("system");
  resetLayout();
}
</script>

<template>
  <Sheet>
    <SheetTrigger as-child>
      <Button size="icon" variant="ghost" type="button" class="rounded-full" aria-label="打开主题设置">
        <Settings aria-hidden="true" />
      </Button>
    </SheetTrigger>
    <SheetContent class="flex flex-col">
      <SheetHeader class="pb-0 text-start">
        <SheetTitle>主题设置</SheetTitle>
        <SheetDescription>调整外观和布局。</SheetDescription>
      </SheetHeader>
      <div class="space-y-6 overflow-y-auto px-4">
        <section>
          <div class="mb-2 flex items-center gap-2 text-sm font-semibold text-muted-foreground">
            主题
            <Button
              v-if="themeChoice !== 'system'"
              type="button"
              size="icon"
              variant="secondary"
              class="size-4 rounded-full"
              aria-label="恢复默认主题"
              @click="applyTheme('system')"
            >
              <RotateCcw class="size-3" />
            </Button>
          </div>
          <div class="grid w-full max-w-md grid-cols-3 gap-4" role="radiogroup" aria-label="主题">
            <button
              v-for="item in themes"
              :key="item.value"
              type="button"
              class="group text-center outline-none"
              role="radio"
              :aria-checked="themeChoice === item.value"
              :aria-label="item.label"
              @click="applyTheme(item.value)"
            >
              <div
                :class="
                  cn(
                    'relative rounded-[6px] ring-1 ring-border',
                    themeChoice === item.value && 'shadow-2xl ring-primary',
                  )
                "
              >
                <CircleCheck
                  v-if="themeChoice === item.value"
                  class="absolute top-0 right-0 size-6 translate-x-1/2 -translate-y-1/2 fill-primary stroke-white"
                />
                <component :is="item.icon" class="w-full" />
              </div>
              <div class="mt-1 text-xs">{{ item.label }}</div>
            </button>
          </div>
        </section>

        <section class="max-md:hidden">
          <div class="mb-2 flex items-center gap-2 text-sm font-semibold text-muted-foreground">
            侧栏
            <Button
              v-if="sidebarVariant !== 'inset'"
              type="button"
              size="icon"
              variant="secondary"
              class="size-4 rounded-full"
              aria-label="恢复默认侧栏"
              @click="setSidebarVariant('inset')"
            >
              <RotateCcw class="size-3" />
            </Button>
          </div>
          <div class="grid w-full max-w-md grid-cols-3 gap-4" role="radiogroup" aria-label="侧栏">
            <button
              v-for="item in variants"
              :key="item.value"
              type="button"
              class="text-center"
              role="radio"
              :aria-checked="sidebarVariant === item.value"
              :aria-label="item.label"
              @click="setSidebarVariant(item.value)"
            >
              <div
                :class="
                  cn(
                    'relative rounded-[6px] ring-1 ring-border',
                    sidebarVariant === item.value && 'shadow-2xl ring-primary',
                  )
                "
              >
                <CircleCheck
                  v-if="sidebarVariant === item.value"
                  class="absolute top-0 right-0 size-6 translate-x-1/2 -translate-y-1/2 fill-primary stroke-white"
                />
                <component
                  :is="item.icon"
                  :class="
                    cn(
                      'w-full fill-primary stroke-primary',
                      sidebarVariant !== item.value && 'fill-muted-foreground stroke-muted-foreground',
                    )
                  "
                />
              </div>
              <div class="mt-1 text-xs">{{ item.label }}</div>
            </button>
          </div>
        </section>

        <section class="max-md:hidden">
          <div class="mb-2 flex items-center gap-2 text-sm font-semibold text-muted-foreground">
            布局
            <Button
              v-if="layoutValue() !== 'default'"
              type="button"
              size="icon"
              variant="secondary"
              class="size-4 rounded-full"
              aria-label="恢复默认布局"
              @click="chooseLayout('default')"
            >
              <RotateCcw class="size-3" />
            </Button>
          </div>
          <div class="grid w-full max-w-md grid-cols-3 gap-4" role="radiogroup" aria-label="布局">
            <button
              v-for="item in layouts"
              :key="item.value"
              type="button"
              class="text-center"
              role="radio"
              :aria-checked="layoutValue() === item.value"
              :aria-label="item.label"
              @click="chooseLayout(item.value)"
            >
              <div
                :class="
                  cn(
                    'relative rounded-[6px] ring-1 ring-border',
                    layoutValue() === item.value && 'shadow-2xl ring-primary',
                  )
                "
              >
                <CircleCheck
                  v-if="layoutValue() === item.value"
                  class="absolute top-0 right-0 size-6 translate-x-1/2 -translate-y-1/2 fill-primary stroke-white"
                />
                <component
                  :is="item.icon"
                  :class="
                    cn(
                      'w-full fill-primary stroke-primary',
                      layoutValue() !== item.value && 'fill-muted-foreground stroke-muted-foreground',
                    )
                  "
                />
              </div>
              <div class="mt-1 text-xs">{{ item.label }}</div>
            </button>
          </div>
        </section>

        <section>
          <div class="mb-2 flex items-center gap-2 text-sm font-semibold text-muted-foreground">
            方向
            <Button
              v-if="direction !== 'ltr'"
              type="button"
              size="icon"
              variant="secondary"
              class="size-4 rounded-full"
              aria-label="恢复从左到右"
              @click="setDirection('ltr')"
            >
              <RotateCcw class="size-3" />
            </Button>
          </div>
          <div class="grid w-full max-w-md grid-cols-3 gap-4" role="radiogroup" aria-label="方向">
            <button
              v-for="item in directions"
              :key="item.value"
              type="button"
              class="text-center"
              role="radio"
              :aria-checked="direction === item.value"
              :aria-label="item.label"
              @click="setDirection(item.value)"
            >
              <div
                :class="
                  cn(
                    'relative rounded-[6px] ring-1 ring-border',
                    direction === item.value && 'shadow-2xl ring-primary',
                  )
                "
              >
                <CircleCheck
                  v-if="direction === item.value"
                  class="absolute top-0 right-0 size-6 translate-x-1/2 -translate-y-1/2 fill-primary stroke-white"
                />
                <IconDir
                  :dir="item.value"
                  :class="
                    cn(
                      'w-full fill-primary stroke-primary',
                      direction !== item.value && 'fill-muted-foreground stroke-muted-foreground',
                    )
                  "
                />
              </div>
              <div class="mt-1 text-xs">{{ item.label }}</div>
            </button>
          </div>
        </section>
      </div>
      <SheetFooter class="gap-2">
        <Button variant="destructive" type="button" @click="resetAll">重置</Button>
      </SheetFooter>
    </SheetContent>
  </Sheet>
</template>
