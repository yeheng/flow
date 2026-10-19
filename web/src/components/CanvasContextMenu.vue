<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref } from "vue";
import type { OverlayEntry } from "../state/overlays";

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

/** 栈上 context-menu 条目的载荷（渲染方 FlowCanvas） */
export interface MenuPayload {
  x: number;
  y: number;
  entries: MenuEntry[];
}

const props = defineProps<{ entry: OverlayEntry<MenuPayload> }>();

const root = ref<HTMLElement>();
const pos = ref({ left: props.entry.payload.x, top: props.entry.payload.y });
const entries = computed(() => props.entry.payload.entries);
/** hover 展开的子菜单：key + 是否向左翻转（右侧放不下时） */
const openSub = ref<{ key: string; flip: boolean } | null>(null);

/** 唯一关闭出口：Esc 由全局弹层栈关栈顶（=本条目），此处不再监听 Escape */
function close(): void {
  props.entry.close();
}

onMounted(() => {
  // 越界钳制：菜单尽量完整落在视口内
  const rect = root.value?.getBoundingClientRect();
  if (rect) {
    pos.value.left = Math.max(4, Math.min(pos.value.left, window.innerWidth - rect.width - 8));
    pos.value.top = Math.max(4, Math.min(pos.value.top, window.innerHeight - rect.height - 8));
  }
  // 打开即聚焦首项，键盘可以直接操作
  focusItemAt(0);
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
  close();
}

/** 可聚焦菜单项（role=menuitem 且未禁用）；子菜单项只在展开时存在于 DOM */
function itemEls(): HTMLElement[] {
  return Array.from(root.value?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? []).filter(
    (el) => !el.classList.contains("disabled"),
  );
}

/** 漫游 tabindex：同一时刻只有当前项在 Tab 序列里 */
function focusItem(el: HTMLElement): void {
  for (const x of itemEls()) x.setAttribute("tabindex", x === el ? "0" : "-1");
  el.focus();
}

function focusItemAt(i: number): void {
  const els = itemEls();
  if (els.length === 0) return;
  focusItem(els[((i % els.length) + els.length) % els.length]);
}

/** 焦点进当前展开的子菜单首项 */
function focusFirstSub(): void {
  const first = root.value?.querySelector<HTMLElement>('.ctx-sub [role="menuitem"]:not(.disabled)');
  if (first) focusItem(first);
}

/** 菜单内键盘导航；Escape 不在此处理——全局弹层栈关栈顶（=本菜单） */
function onKeydown(e: KeyboardEvent): void {
  const els = itemEls();
  const cur = els.indexOf(document.activeElement as HTMLElement);
  switch (e.key) {
    case "ArrowDown":
      e.preventDefault();
      focusItemAt(cur + 1);
      return;
    case "ArrowUp":
      e.preventDefault();
      focusItemAt(cur <= 0 ? els.length - 1 : cur - 1);
      return;
    case "Home":
      e.preventDefault();
      focusItemAt(0);
      return;
    case "End":
      e.preventDefault();
      focusItemAt(els.length - 1);
      return;
    case "Enter":
    case " ": {
      // 焦点在项上触发该项；焦点不在任何元素上（刚打开）时触发首项
      const active = document.activeElement;
      const target = els[cur] ?? (active === document.body ? els[0] : undefined);
      if (!target) return;
      e.preventDefault();
      target.click();
      return;
    }
  }
}

/** 执行菜单项动作后关闭（close 是唯一关闭出口） */
function run(item: MenuEntry | MenuSubEntry): void {
  if (item.disabled) return;
  item.action?.();
  close();
}

function onItemEnter(entry: MenuEntry, el: HTMLElement): void {
  if (!entry.children || entry.children.length === 0) {
    openSub.value = null;
    return;
  }
  openSubmenu(entry, el);
}

function openSubmenu(entry: MenuEntry, el: HTMLElement): void {
  const rect = el.getBoundingClientRect();
  openSub.value = { key: entry.key, flip: rect.right + 230 > window.innerWidth };
}

/** 点击/Enter：叶子项执行动作，带子菜单的项展开并把焦点交给其首项 */
function onItemClick(entry: MenuEntry, el: HTMLElement): void {
  if (entry.disabled) return;
  if (entry.children && entry.children.length > 0) {
    openSubmenu(entry, el);
    void nextTick().then(focusFirstSub);
    return;
  }
  run(entry);
}
</script>

<template>
  <div
    ref="root"
    class="ctx-menu"
    role="menu"
    :style="{ left: `${pos.left}px`, top: `${pos.top}px` }"
    tabindex="-1"
    @contextmenu.prevent
  >
    <template v-for="e in entries" :key="e.key">
      <div v-if="!e.label" class="ctx-sep" role="separator" />
      <div
        v-else
        class="ctx-item"
        role="menuitem"
        tabindex="-1"
        :class="{ danger: e.danger, disabled: e.disabled, 'has-sub': !!e.children }"
        :aria-disabled="e.disabled ? 'true' : undefined"
        :aria-haspopup="e.children ? 'menu' : undefined"
        :aria-expanded="e.children ? openSub?.key === e.key : undefined"
        @pointerenter="onItemEnter(e, $event.currentTarget as HTMLElement)"
        @click="onItemClick(e, $event.currentTarget as HTMLElement)"
      >
        <span class="ctx-label">{{ e.label }}</span>
        <span v-if="e.hint" class="ctx-hint">{{ e.hint }}</span>
        <span v-if="e.children" class="ctx-arrow" aria-hidden="true">›</span>
        <div
          v-if="e.children && openSub?.key === e.key"
          class="ctx-sub"
          :class="{ flip: openSub.flip }"
          role="menu"
        >
          <template v-for="(s, i) in e.children" :key="s.key">
            <div
              v-if="s.group && s.group !== e.children![i - 1]?.group"
              class="ctx-group"
              role="presentation"
            >
              {{ s.group }}
            </div>
            <div
              class="ctx-item"
              role="menuitem"
              tabindex="-1"
              :class="{ disabled: s.disabled }"
              :aria-disabled="s.disabled ? 'true' : undefined"
              @click.stop="run(s)"
            >
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
