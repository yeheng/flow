/** Shared typed fields for workflow inputs and import configuration. */
export interface FormField {
  key: string;
  label: string;
  type?: string;
  required?: boolean;
  secret?: boolean;
  help?: string;
  enum?: unknown[];
  defaultValue?: unknown;
}

export function fieldText(value: unknown): string {
  return value === undefined ? "" : typeof value === "string" ? value : JSON.stringify(value);
}

export function parseFormFields(
  fields: FormField[],
  texts: Record<string, string>,
): Record<string, unknown> {
  const result: Record<string, unknown> = Object.create(null);
  for (const field of fields) {
    const raw = texts[field.key] ?? "";
    if (!raw.trim()) {
      if (field.required) throw new Error(`请填写${field.label}`);
      continue;
    }
    let value: unknown = raw;
    if (field.enum || (field.type && field.type !== "string")) {
      try {
        value = JSON.parse(raw);
      } catch {
        throw new Error(`${field.label}：请输入有效的 ${field.type ?? "JSON"} 值`);
      }
    } else if (!field.type) {
      try {
        value = JSON.parse(raw);
      } catch {
        /* Legacy input fields accept text or JSON. */
      }
    }
    const kind = field.type;
    if (
      (kind === "integer" && (typeof value !== "number" || !Number.isInteger(value))) ||
      (kind === "number" && typeof value !== "number") ||
      (kind === "boolean" && typeof value !== "boolean") ||
      (kind === "array" && !Array.isArray(value)) ||
      (kind === "object" &&
        (value === null || typeof value !== "object" || Array.isArray(value))) ||
      (kind === "null" && value !== null)
    )
      throw new Error(`${field.label}：类型应为 ${kind}`);
    if (field.enum && !field.enum.some((v) => JSON.stringify(v) === JSON.stringify(value))) {
      throw new Error(`${field.label}：请选择有效选项`);
    }
    result[field.key] = value;
  }
  return result;
}

type Schema = Record<string, unknown>;
export function schemaObject(value: unknown): Schema {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Schema)
    : {};
}

export function schemaFields(schema: unknown): FormField[] {
  const s = schemaObject(schema);
  const properties = schemaObject(s.properties);
  const required = Array.isArray(s.required)
    ? s.required.filter((k): k is string => typeof k === "string")
    : [];
  return [...new Set([...Object.keys(properties), ...required])].map((key) => {
    const p = schemaObject(properties[key]);
    return {
      key,
      label: typeof p.title === "string" ? p.title : key,
      type: typeof p.type === "string" ? p.type : undefined,
      required: required.includes(key),
      help: typeof p.description === "string" ? p.description : undefined,
      enum: Array.isArray(p.enum) ? p.enum : undefined,
      defaultValue: p.default,
    };
  });
}

/** Mirrors the engine's bounded JSON Schema subset; the server remains authoritative. */
export function validateInputSchema(
  schema: unknown,
  value: unknown,
  path = "$",
  depth = 0,
): string | undefined {
  if (depth > 64) return `${path}：嵌套过深`;
  if (schema === false) return `${path}：不允许此值`;
  const s = schemaObject(schema);
  const kind = s.type;
  const valid =
    kind === undefined ||
    (kind === "null"
      ? value === null
      : kind === "array"
        ? Array.isArray(value)
        : kind === "object"
          ? value !== null && typeof value === "object" && !Array.isArray(value)
          : kind === "integer"
            ? typeof value === "number" && Number.isInteger(value)
            : typeof value === kind);
  if (!valid) return `${path}：类型应为 ${String(kind)}`;
  if (Array.isArray(s.enum) && !s.enum.some((v) => JSON.stringify(v) === JSON.stringify(value)))
    return `${path}：值不在可选项中`;
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const object = value as Record<string, unknown>;
    for (const key of Array.isArray(s.required) ? s.required : []) {
      if (typeof key === "string" && !Object.prototype.hasOwnProperty.call(object, key))
        return `${path}.${key}：必填`;
    }
    const properties = schemaObject(s.properties);
    for (const [key, v] of Object.entries(object)) {
      const child = Object.prototype.hasOwnProperty.call(properties, key)
        ? properties[key]
        : s.additionalProperties;
      const error = validateInputSchema(child, v, `${path}.${key}`, depth + 1);
      if (error) return error;
    }
  }
  if (Array.isArray(value)) {
    if (
      (typeof s.minItems === "number" && value.length < s.minItems) ||
      (typeof s.maxItems === "number" && value.length > s.maxItems)
    )
      return `${path}：数组长度超出范围`;
    for (let i = 0; i < value.length; i++) {
      const error = validateInputSchema(s.items, value[i], `${path}[${i}]`, depth + 1);
      if (error) return error;
    }
  }
  if (
    typeof value === "string" &&
    ((typeof s.minLength === "number" && [...value].length < s.minLength) ||
      (typeof s.maxLength === "number" && [...value].length > s.maxLength))
  )
    return `${path}：文本长度超出范围`;
  if (
    typeof value === "number" &&
    ((typeof s.minimum === "number" && value < s.minimum) ||
      (typeof s.maximum === "number" && value > s.maximum))
  )
    return `${path}：数值超出范围`;
  return undefined;
}
