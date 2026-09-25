import js from "@eslint/js";
import ts from "typescript-eslint";
import vue from "eslint-plugin-vue";
import prettier from "eslint-config-prettier";

export default ts.config(
  {
    ignores: ["dist", "node_modules"],
  },
  js.configs.recommended,
  ...ts.configs.recommended,
  ...vue.configs["flat/recommended"],
  {
    files: ["**/*.vue"],
    languageOptions: {
      parserOptions: {
        parser: ts.parser,
      },
    },
  },
  {
    rules: {
      // 从严但不为难
      // DOM/定时器等浏览器全局由 TS lib 检查，no-undef 对 TS 只会误报
      "no-undef": "off",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "vue/multi-word-component-names": "off",
      "vue/require-default-prop": "off",
      // 内联 SVG 图标是硬编码静态内容，非用户输入
      "vue/no-v-html": "off",
    },
  },
  prettier,
);
