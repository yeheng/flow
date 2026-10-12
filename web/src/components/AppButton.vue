<script setup lang="ts">
import { ref } from "vue";

// 全局统一按钮：variant 映射 style.css 的标准按钮样式，调用方不再散写 class 组合
// （"link danger" 这类组合的特异性冲突即源于此）。type/disabled/title/@click 等
// 原生属性经 fallthrough 透传到根 <button>。
withDefaults(
  defineProps<{
    variant?: "default" | "primary" | "danger" | "ghost" | "link" | "link-danger";
  }>(),
  { variant: "default" },
);

const root = ref<HTMLButtonElement | null>(null);
defineExpose({ focus: () => root.value?.focus() });
</script>

<template>
  <button
    ref="root"
    :class="{
      primary: variant === 'primary',
      danger: variant === 'danger' || variant === 'link-danger',
      ghost: variant === 'ghost',
      link: variant === 'link' || variant === 'link-danger',
    }"
  >
    <slot />
  </button>
</template>
