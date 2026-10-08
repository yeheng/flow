<script setup lang="ts">
import { onMounted, onUnmounted, ref } from "vue";

/** 子菜单项：group 相邻不同时渲染分组小标题（「添加节点」按分类分组） */
export interface MenuSubEntry {
  key: string;
  label: string;
  group?: string;
  disabled?: boolean;
  action: () => void;
}

/** label 缺省表示分隔线；带 children 的项 hover 展开子菜单 */
export interface MenuEntry {
  key: string;
  label?: string;
  /** 右侧快捷键提示（如 ⌘C） */
  hint?: string;
  danger?: boolean;
  disabled?: boolean;
  action?: () => void;
  children?: MenuSubEntry[];
}

const props = defineProps<{ x: number; y: number; entries: MenuEntry[] }>();
const emit = defineEmits<{ close: [] }>();

const root = ref<HTMLElement>();
const pos = ref({ left: props.x, top: props.y });
/** hover 展开的子菜单：key + 是否向左翻转（右侧放不下时） */
const openSub = ref<{ key: string; flip: boolean } | null>(null);

onMounted(() => {
  // 越界钳制：菜单尽量完整落在视口内
  const rect = root.value?.getBoundingClientRect();
  if (rect) {
    pos.value.left = Math.max(4, Math.min(props.x, window.innerWidth - rect.width - 8));
    pos.value.top = Math.max(4, Math.min(props.y, window.innerHeight - rect.height - 8));
  }
  window.addEventListener("pointerdown", onPointerDownOutside, true);
  window.addEventListener("keydown", onKeydown, true);
});

onUnmounted(() => {
  window.removeEventListener("pointerdown", onPointerDownOutside, true);
  window.removeEventListener("keydown", onKeydown, true);
});

function onPointerDownOutside(e: PointerEvent): void {
  // 点在菜单内：交给各项的 click 处理（pointerdown 先于 click，不能提前卸载）
  if (root.value?.contains(e.target as Node)) return;
  emit("close");
}

function onKeydown(e: KeyboardEvent): void {
  if (e.key === "Escape") emit("close");
}

function run(entry: MenuEntry | MenuSubEntry): void {
  if (entry.disabled) return;
  entry.action?.();
  emit("close");
}

function onItemEnter(entry: MenuEntry, el: HTMLElement): void {
  if (!entry.children || entry.children.length === 0) {
    openSub.value = null;
    return;
  }
  const rect = el.getBoundingClientRect();
  openSub.value = { key: entry.key, flip: rect.right + 230 > window.innerWidth };
}
</script>

<template>
  <div
    ref="root"
    class="ctx-menu"
    :style="{ left: `${pos.left}px`, top: `${pos.top}px` }"
    tabindex="-1"
    @contextmenu.prevent
  >
    <template v-for="e in entries" :key="e.key">
      <div v-if="!e.label" class="ctx-sep" />
      <div
        v-else
        class="ctx-item"
        :class="{ danger: e.danger, disabled: e.disabled, 'has-sub': !!e.children }"
        @pointerenter="onItemEnter(e, $event.currentTarget as HTMLElement)"
        @click="!e.children && run(e)"
      >
        <span class="ctx-label">{{ e.label }}</span>
        <span v-if="e.hint" class="ctx-hint">{{ e.hint }}</span>
        <span v-if="e.children" class="ctx-arrow" aria-hidden="true">›</span>
        <div v-if="e.children && openSub?.key === e.key" class="ctx-sub" :class="{ flip: openSub.flip }">
          <template v-for="(s, i) in e.children" :key="s.key">
            <div v-if="s.group && s.group !== e.children![i - 1]?.group" class="ctx-group">
              {{ s.group }}
            </div>
            <div class="ctx-item" :class="{ disabled: s.disabled }" @click.stop="run(s)">
              <span class="ctx-label">{{ s.label }}</span>
            </div>
          </template>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.ctx-menu {
  position: fixed;
  z-index: 300;
  min-width: 170px;
  max-width: 260px;
  padding: 5px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 8px;
  box-shadow: 0 8px 32px var(--shadow-hover);
  font-size: 12px;
  color: var(--text1);
  user-select: none;
}

.ctx-item {
  position: relative;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 6px 10px;
  border-radius: 6px;
  cursor: pointer;
  white-space: nowrap;
}

.ctx-item:hover:not(.disabled) {
  background: var(--surface2);
}

.ctx-item.disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.ctx-item.danger {
  color: var(--danger);
}

.ctx-item.danger:hover:not(.disabled) {
  background: rgba(248, 81, 73, 0.12);
}

.ctx-label {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
}

.ctx-hint {
  color: var(--text3);
  font-size: 11px;
}

.ctx-arrow {
  color: var(--text3);
}

.ctx-sep {
  height: 1px;
  margin: 4px 6px;
  background: var(--border);
}

.ctx-sub {
  position: absolute;
  top: -5px;
  left: calc(100% + 4px);
  z-index: 1;
  min-width: 170px;
  max-height: 60vh;
  overflow-y: auto;
  padding: 5px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 8px;
  box-shadow: 0 8px 32px var(--shadow-hover);
}

.ctx-sub.flip {
  left: auto;
  right: calc(100% + 4px);
}

.ctx-group {
  padding: 6px 10px 3px;
  color: var(--text3);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.ctx-group:first-child {
  padding-top: 2px;
}
</style>
