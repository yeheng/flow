<script setup lang="ts">
import type { FormField } from "../state/form-fields";
defineProps<{ fields: FormField[]; values: Record<string, string> }>();
defineEmits<{ change: [key: string, value: string] }>();
</script>

<template>
  <div class="modal-fields">
    <label v-for="field in fields" :key="field.key">
      <span>{{ field.label }}<span v-if="field.required" class="required"> *</span></span>
      <select
        v-if="field.enum"
        :value="values[field.key]"
        @change="$emit('change', field.key, ($event.target as HTMLSelectElement).value)"
      >
        <option value="">请选择</option>
        <option v-for="(value, i) in field.enum" :key="i" :value="JSON.stringify(value)">
          {{ String(value) }}
        </option>
      </select>
      <select
        v-else-if="field.type === 'boolean'"
        :value="values[field.key]"
        @change="$emit('change', field.key, ($event.target as HTMLSelectElement).value)"
      >
        <option value="">请选择</option>
        <option value="true">是</option>
        <option value="false">否</option>
      </select>
      <textarea
        v-else-if="['json', 'object', 'array'].includes(field.type ?? '')"
        :value="values[field.key]"
        rows="4"
        spellcheck="false"
        @input="$emit('change', field.key, ($event.target as HTMLTextAreaElement).value)"
      ></textarea>
      <input
        v-else
        :type="
          field.secret
            ? 'password'
            : ['number', 'integer'].includes(field.type ?? '')
              ? 'number'
              : 'text'
        "
        :step="field.type === 'integer' ? '1' : 'any'"
        :value="values[field.key]"
        :autocomplete="field.secret ? 'new-password' : 'off'"
        @input="$emit('change', field.key, ($event.target as HTMLInputElement).value)"
      />
      <small v-if="field.help">{{ field.help }}</small>
    </label>
  </div>
</template>

<style scoped>
.modal-fields {
  display: grid;
  gap: 12px;
}
label {
  display: grid;
  gap: 5px;
}
input,
select,
textarea {
  width: 100%;
  box-sizing: border-box;
}
small {
  color: var(--text2);
  white-space: pre-wrap;
}
.required {
  color: var(--danger);
}
</style>
